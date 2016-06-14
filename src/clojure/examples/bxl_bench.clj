(ns examples.bxl-bench
  (:require [tamura.config :as cfg]
            [tamura.core :as t]
            [tamura.node :as n]
            [tamura.runtimes.spark :as spark]
            [clj-time.core :as time]
            [clj-time.format :as f])
  (:import [redis.clients.jedis Jedis]
           [examples.bxldirect BxlDirect])
  (:use [examples.bxl-helper])
  (:gen-class))

;(def redis-host "134.184.49.17")
(def redis-host "localhost")
(def redis-key "bxlqueue")
(def throttle-time (atom 1000))

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
    (t/do-apply (comp -append first) r)))

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
                       (continuation)
                       (do
                         (Thread/sleep 100)
                         (recur))))))
    (.start)))

(defn clear-queue
  [conn]
  (.del conn redis-key))

(def start-time (atom nil))

(defn setup-java!
  []
  (BxlDirect/setSparkContext spark/sc)
  (BxlDirect/setCheckpointDir "/tmp/checkpoint")
  (BxlDirect/setRedisHost redis-host)
  (BxlDirect/setRedisKey redis-key)
  (BxlDirect/setDuration @throttle-time))

(defn test5
  [conn users updates next]
  (reset! max-directions [])
  (BxlDirect/spawnMessages conn users updates)
  (BxlDirect/start)
  (reset! start-time (System/nanoTime))
  (create-poll-thread
    conn
    (fn []
      (let [t (/ (double (- (System/nanoTime) @start-time)) 1000000.0)]
        (println "Done test 5 in" t)
        (println)
        (println (count @max-directions) "elements in max-directions")
        (BxlDirect/stop)
        (when next
          ((first next) conn users updates (rest next)))))))

(defn test4
  [conn users updates next]
  (reset! max-directions [])
  (t/reset!)
  (swap! cfg/config assoc :throttle @throttle-time)
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
        (println)
        (println (count @max-directions) "elements in max-directions")
        (t/stop!)
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
        (println)
        (println (count @max-directions) "elements in max-directions")
        (t/stop!)
        (when next
          ((first next) conn users updates (rest next))))))
  (println "Started test 3"))

(defn test2
  [conn users updates next]
  (reset! max-directions [])
  (t/reset!)
  (swap! cfg/config assoc :throttle @throttle-time)
  (push-messages conn users updates)
  (create-graph)
  (t/start!)
  (reset! start-time (System/nanoTime))
  (create-poll-thread
    conn
    (fn []
      (let [t (/ (double (- (System/nanoTime) @start-time)) 1000000.0)]
        (println "Done test 2 in" t)
        (println)
        (println (count @max-directions) "elements in max-directions")
        (t/stop!)
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
        (println)
        (println (count @max-directions) "elements in max-directions")
        (t/stop!)
        (when next
          ((first next) conn users updates (rest next))))))
  (println "Started test 1"))

(defn stopper
  [& args]
  (println "Done!")
  (System/exit 0))

(defn do-tests
  [conn users updates tests]
  (apply (first tests) [conn users updates (concat (rest tests) [stopper])]))

(defn -main
  [users updates & [cores throttle]]
  (let [users (read-string users)
        updates (read-string updates)
        conn (Jedis. redis-host)
        cores (if cores (read-string cores) 0)
        cores (if (= cores 0) "*" cores)
        throttle (if throttle (read-string throttle) 1000)]
    (reset! throttle-time throttle)
    (swap! cfg/config assoc-in [:spark :master] (str "local[" cores "]"))
    (spark/setup-spark!)
    (setup-java!)
    (println "Starting tests...")
    ;(push-messages conn users updates)
    (do-tests conn users updates
              [test2 test2 test2 test2 test2
               test4 test4 test4 test4 test4
               test5 test5 test5 test5 test5])))