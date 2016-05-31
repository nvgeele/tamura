(ns examples.bxl-direct
  (:require [tamura.core :as t]
            [clj-time.core :as time]
            [examples.spawner :as s]
            [clj-time.format :as f])
  (:import [redis.clients.jedis JedisPool Jedis]))

(def redis-host "134.184.49.17")
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

(comment
  (t/defsig max-direction (-> (t/redis redis-host redis-key :key :user-id :buffer 2)
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
                                            (if (> c1 c2) t1 t2)))
                                        [nil -1])))
  (t/print-signal (t/throttle max-direction 1000)))

;;;;;;;;;;;;;;;

(defn spawn-thread
  [host queue id]
  (let [conn (Jedis. host)]
    (t/threadloop []
      (.rpush conn queue (into-array String [(str {:user-id  id
                                                   :position [(Math/random) (Math/random)]
                                                   :time     (str (time/now))})]))
      (Thread/sleep 1000)
      (recur))))

(defn spawn-threads
  [host queue num]
  (doseq [i (range 0 num)]
    (spawn-thread host queue i)))

(.addShutdownHook (Runtime/getRuntime)
                  (Thread. (fn []
                             (.del (Jedis. redis-host) redis-key)
                             (println "Emptied queue")
                             (flush))))

(defn -main
  [& args]
  (spawn-threads redis-host redis-key 25)
  (t/start!)
  (println "Ready"))