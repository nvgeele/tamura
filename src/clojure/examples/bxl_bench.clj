(ns examples.bxl-bench
  (:require [tamura.config :as cfg]
            [tamura.core :as t]
            [tamura.node :as n]
            [tamura.profile :as profile]
            [tamura.runtimes.spark :as spark]
            [clj-time.core :as time]
            [clj-time.format :as f])
  (:import [redis.clients.jedis Jedis]
           [examples.bxldirect BxlDirect]
           [java.lang Math])
  (:use [examples.bxl-helper])
  (:gen-class))

;(def redis-host "134.184.49.17")
(def redis-host "localhost")
(def redis-key "bxlqueue")
(def redis-out-key "bxlout")
(def throttle-time (atom 1000))

(def redis-out? false)

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
  (let [redis-input (t/redis redis-host redis-key :key :user-id :buffer 2)
        out (-> redis-input
                (t/filter-key-size  2)
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
    ;(t/print out)
    (if redis-out?
      (t/redis-out out redis-host redis-out-key)
      (t/do-apply (comp -append first) out))))

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

(defn clear-out-queue!
  []
  (let [conn (Jedis. redis-host)]
    (.del conn redis-out-key)
    (.close conn)))

(defn out-queue-count
  []
  (let [conn (Jedis. redis-host)
        count (.llen conn redis-out-key)]
    (.close conn)
    count))

(defn setup-java!
  []
  (BxlDirect/setSparkContext spark/sc)
  (BxlDirect/setCheckpointDir "/tmp/checkpoint")
  (BxlDirect/setRedisHost redis-host)
  (BxlDirect/setRedisKey redis-key)
  (BxlDirect/setDuration @throttle-time)
  (BxlDirect/setRedisOutKey redis-out-key)
  (BxlDirect/setRedisOut redis-out?))

(def start-time (atom nil))
(def times (atom {}))

(defn start-time!
  []
  (reset! start-time (System/nanoTime)))

(defn stop-time!
  [key]
  (let [t (/ (double (- (System/nanoTime) @start-time)) 1000000.0)]
    (swap! times update key conj t)
    t))

(defn continue
  [conn users updates next]
  (t/reset!)
  (reset! max-directions [])
  (clear-queue conn)
  (clear-out-queue!)
  (cfg/reset-config!)
  (when next
    ((first next) conn users updates (rest next))))

(defn pure-spark-streaming
  [conn users updates next]
  (BxlDirect/spawnMessages conn users updates)
  (BxlDirect/start)
  (start-time!)
  (create-poll-thread
    conn
    (fn []
      (let [t (stop-time! :pure-spark-streaming)]
        (BxlDirect/stop)
        (println "Done test pure-spark-streaming in" t)
        (println)
        (if redis-out?
          (println (out-queue-count) "elements in out-queue")
          (println (count @max-directions) "elements in max-directions"))
        (continue conn users updates next))))
  (println "Started test pure-spark-streaming"))

(defn spark-runtime-throttled-receivers
  [conn users updates next]
  (cfg/set-runtime! :spark)
  (cfg/set-throttle! @throttle-time)
  (cfg/set-spark-enable-stream-receivers! true)
  (push-messages conn users updates)
  (create-graph)
  (t/start!)
  (start-time!)
  (create-poll-thread
    conn
    (fn []
      (let [t (stop-time! :spark-runtime-throttled-receivers)]
        (t/stop!)
        (println "Done test spark-runtime-throttled-receivers in" t)
        (println)
        (if redis-out?
          (println (out-queue-count) "elements in out-queue")
          (println (count @max-directions) "elements in max-directions"))
        (when profile/profile?
          (println "Collect time:" (do (await spark/collect-times) @spark/collect-times))
          (println "Parallelize time:" (do (await spark/parallelize-times) @spark/parallelize-times))
          (send spark/collect-times (fn [& args] 0))
          (send spark/parallelize-times (fn [& args] 0)))
        (continue conn users updates next))))
  (println "Started test spark-runtime-throttled-receivers"))

(defn spark-runtime-throttled
  [conn users updates next]
  (cfg/set-runtime! :spark)
  (cfg/set-throttle! @throttle-time)
  (push-messages conn users updates)
  (create-graph)
  (t/start!)
  (start-time!)
  (create-poll-thread
    conn
    (fn []
      (let [t (stop-time! :spark-runtime-throttled)]
        (t/stop!)
        (println "Done test spark-runtime-throttled in" t)
        (println)
        (if redis-out?
          (println (out-queue-count) "elements in out-queue")
          (println (count @max-directions) "elements in max-directions"))
        (when profile/profile?
          (println "Collect time:" (do (await spark/collect-times) @spark/collect-times))
          (println "Parallelize time:" (do (await spark/parallelize-times) @spark/parallelize-times))
          (send spark/collect-times (fn [& args] 0))
          (send spark/parallelize-times (fn [& args] 0)))
        (continue conn users updates next))))
  (println "Started test spark-runtime-throttled"))

(defn spark-runtime-no-throttle
  [conn users updates next]
  (cfg/set-runtime! :spark)
  (cfg/set-throttle! false)
  (push-messages conn users updates)
  (create-graph)
  (t/start!)
  (start-time!)
  (create-poll-thread
    conn
    (fn []
      (let [t (stop-time! :spark-runtime-no-throttle)]
        (t/stop!)
        (println "Done test spark-runtime-no-throttle in" t)
        (println)
        (if redis-out?
          (println (out-queue-count) "elements in out-queue")
          (println (count @max-directions) "elements in max-directions"))
        (when profile/profile?
          (println "Collect time:" (do (await spark/collect-times) @spark/collect-times))
          (println "Parallelize time:" (do (await spark/parallelize-times) @spark/parallelize-times))
          (send spark/collect-times (fn [& args] 0))
          (send spark/parallelize-times (fn [& args] 0)))
        (continue conn users updates next))))
  (println "Started test spark-runtime-no-throttle"))

(defn clj-runtime-throttled
  [conn users updates next]
  (cfg/set-runtime! :clj)
  (cfg/set-throttle! @throttle-time)
  (push-messages conn users updates)
  (create-graph)
  (t/start!)
  (start-time!)
  (create-poll-thread
    conn
    (fn []
      (let [t (stop-time! :clj-runtime-throttled)]
        (t/stop!)
        (println "Done test clj-runtime-throttled in" t)
        (println)
        (if redis-out?
          (println (out-queue-count) "elements in out-queue")
          (println (count @max-directions) "elements in max-directions"))
        (continue conn users updates next))))
  (println "Started test clj-runtime-throttled"))

(defn clj-runtime-no-throttle
  [conn users updates next]
  (cfg/set-runtime! :clj)
  (cfg/set-throttle! false)
  (push-messages conn users updates)
  (create-graph)
  (t/start!)
  (start-time!)
  (create-poll-thread
    conn
    (fn []
      (let [t (stop-time! :clj-runtime-no-throttle)]
        (t/stop!)
        (println "Done test clj-runtime-no-throttle in" t)
        (println)
        (if redis-out?
          (println (out-queue-count) "elements in out-queue")
          (println (count @max-directions) "elements in max-directions"))
        (continue conn users updates next))))
  (println "Started test clj-runtime-no-throttle"))

(defn mean
  [vals]
  (/ (apply + vals) (count vals)))

(defn standard-deviation
  [vals]
  (let [c (count vals)
        avg (/ (apply + vals) c)]
    (Math/sqrt (/ (reduce + (map #(* (- % avg) (- % avg)) vals)) (- c 1)))))

(defn report
  [conn users updates & args]
  (println "-------------------------")
  (println "Users:" users)
  (println "Updates per user:" updates)
  (println "Total messages:" (* users updates))
  (println "Redis output:" redis-out?)
  (println "Throttle time:" @throttle-time)
  (println "Spark master:" (cfg/spark-master))
  (println)
  (doseq [[k times] @times]
    (let [avg (mean times)
          dev (standard-deviation times)]
      (println (format "Test `%s' average time: %.2f ± %.2f" k avg dev))))
  (println "-------------------------")
  (System/exit 0))

(defn do-tests
  [conn users updates tests]
  (apply (first tests) [conn users updates (concat (rest tests) [report])]))

(def test-fns
  [clj-runtime-no-throttle
   clj-runtime-throttled
   spark-runtime-no-throttle
   spark-runtime-throttled
   spark-runtime-throttled-receivers
   pure-spark-streaming])

(defn -main
  [users updates & [cores throttle redis-output?]]
  (let [users (read-string users)
        updates (read-string updates)
        conn (Jedis. redis-host)
        cores (if cores (read-string cores) 0)
        cores (if (= cores 0) "*" cores)
        throttle (if throttle (read-string throttle) 1000)
        tests [0 10 0 10 10 10]]
    (alter-var-root (var redis-out?) (constantly (if (read-string redis-output?) true false)))
    (reset! throttle-time throttle)
    (swap! cfg/config assoc-in [:spark :master] (str "local[" cores "]"))
    (spark/setup-spark!)
    (setup-java!)
    (println "Starting tests...")
    (do-tests conn users updates
              (mapcat #(repeat %2 (get test-fns %1)) (range 0 (count test-fns)) tests))))