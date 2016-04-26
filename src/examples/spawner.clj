(ns examples.spawner
  (:require [tamura.core :as t])
  (:import [redis.clients.jedis JedisPool Jedis]))

(defn spawn-thread
  [host queue id]
  (let [conn (Jedis. host)]
    (t/threadloop []
      (.rpush conn queue (into-array String [(str {:user-id id :position [(Math/random) (Math/random)]})]))
      (Thread/sleep 1000)
      (recur))))

(defn spawn-threads
  [host queue num]
  (doall (for [i (range 0 num)]
           (spawn-thread host queue i))))

(defn -main
  [& args]
  (spawn-threads "localhost" "bxlqueue" 25)
  (println "Done!"))