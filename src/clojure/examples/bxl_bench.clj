(ns examples.bxl-bench
  (:require [tamura.config :as cfg]
            [tamura.core :as t]
            [tamura.node :as n]
            [clj-time.core :as time]
            [clj-time.format :as f])
  (:import [redis.clients.jedis Jedis])
  (:gen-class))

;(def redis-host "134.184.49.17")
(def redis-host "localhost")
(def redis-key "bxlqueue")

(defn calculate-direction
  [[cur_lat cur_lon] [pre_lat pre_lon]]
  (let [y (* (Math/sin (- cur_lon pre_lon)) (Math/cos cur_lat))
        x (- (* (Math/cos pre_lat) (Math/sin cur_lat))
             (* (Math/sin pre_lat) (Math/cos cur_lat) (Math/cos (- cur_lon pre_lon))))
        bearing (Math/atan2 y x)
        deg (mod (+ (* bearing (/ 180.0 Math/PI)) 360) 360)]
    (cond (and (>= deg 315.) (<= deg 45.)) :east
          (and (>= deg 45.) (<= deg 135.)) :north
          (and (>= deg 135.) (<= deg 225.)) :west
          :else :south)))

(def max-directions (atom []))

;; NOTE: because we are in a DSL we can do this...
(defn create-appender
  [signal]
  (t/do-apply #(swap! max-directions conj (first %)) signal))

(defn create-graph
  []
  (let [r (-> (t/redis redis-host redis-key :key :user-id :buffer 2)
              (t/filter-key-size 2)
              (t/reduce-by-key #(let [t1 (f/parse (:time %1))
                                      t2 (f/parse (:time %2))]
                                 (if (time/before? t1 t2)
                                   (calculate-direction (:position %2) (:position %1))
                                   (calculate-direction (:position %1) (:position %2)))))
              (t/hash-to-multiset)
              (t/map second)
              (t/multiplicities)
              (t/reduce (fn [t1 t2]
                          (let [[d1 c1] t1
                                [d2 c2] t2]
                            (if (> c1 c2) t1 t2)))))]
    ;(t/print r)
    (create-appender r)))

;;;;;;;;;;;;;;;

(defn push-messages
  [conn users updates]
  (let [user-ids (range 0 users)
        start-time (time/now)]
    (dotimes [n updates]
      (doseq [id user-ids]
        (let [t (time/plus start-time (time/seconds n))]
          (.rpush conn
                  redis-key
                  (into-array String [(str {:user-id  id
                                            :position [(Math/random) (Math/random)]
                                            :time     (str t)})])))))
    (println "Pushed messages!")))

(defn create-poll-thread
  [conn continuation]
  (doto (Thread. (fn []
                   (loop []
                     (if (= (.llen conn redis-key) 0)
                       (do
                         (t/stop!)
                         (continuation))
                       (do
                         (Thread/sleep 100)
                         (recur))))))
    (.start)))

(defn clear-queue
  [conn]
  (.del conn redis-key))

(def start-time (atom nil))

;; (/ (double (- (. System (nanoTime)) start#)) 1000000.0)

(defn test4
  [conn users updates next]
  (reset! max-directions [])
  (t/reset!)
  (swap! cfg/config assoc :throttle 1000)
  (swap! cfg/config assoc :runtime :spark)
  (push-messages conn users updates)
  (create-graph)
  (t/start!)
  (reset! start-time (System/nanoTime))
  (create-poll-thread
    conn
    (fn []
      (let [t (/ (double (- (System/nanoTime) @start-time)) 1000000.0)]
        (println "Done test 4 in" t)
        (println (count @max-directions) "elements in max-directions")
        (when next
          ((first next) conn users updates (rest next))))))
  (println "Started test 4"))

(defn test3
  [conn users updates next]
  (reset! max-directions [])
  (t/reset!)
  (swap! cfg/config assoc :throttle false)
  (swap! cfg/config assoc :runtime :spark)
  (push-messages conn users updates)
  (create-graph)
  (t/start!)
  (reset! start-time (System/nanoTime))
  (create-poll-thread
    conn
    (fn []
      (let [t (/ (double (- (System/nanoTime) @start-time)) 1000000.0)]
        (println "Done test 3 in" t)
        (println (count @max-directions) "elements in max-directions")
        (when next
          ((first next) conn users updates (rest next))))))
  (println "Started test 3"))

(defn test2
  [conn users updates next]
  (reset! max-directions [])
  (t/reset!)
  (swap! cfg/config assoc :throttle 1000)
  (push-messages conn users updates)
  (create-graph)
  (t/start!)
  (reset! start-time (System/nanoTime))
  (create-poll-thread
    conn
    (fn []
      (let [t (/ (double (- (System/nanoTime) @start-time)) 1000000.0)]
        (println "Done test 2 in" t)
        (println (count @max-directions) "elements in max-directions")
        (when next
          ((first next) conn users updates (rest next))))))
  (println "Started test 2"))

(defn test1
  [conn users updates next]
  (clear-queue conn)
  (push-messages conn users updates)
  (create-graph)
  (t/start!)
  (reset! start-time (System/nanoTime))
  (create-poll-thread
    conn
    (fn []
      (let [t (/ (double (- (System/nanoTime) @start-time)) 1000000.0)]
        (println "Done test 1 in" t)
        (println (count @max-directions) "elements in max-directions")
        (when next
          ((first next) conn users updates (rest next))))))
  (println "Started test 1"))

(defn stopper
  [& args]
  (println "Done!")
  (System/exit 0))

(defn -main
  [users updates]
  (let [users (read-string users)
        updates (read-string updates)
        conn (Jedis. redis-host)]
    (println "Starting tests...")
    (test2 conn users updates [test4 stopper])))