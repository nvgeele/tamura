(ns examples.spawner
  (:require [clj-time.core :as time])
  (:import [redis.clients.jedis JedisPool Jedis]))

(defmacro thread
  [& body]
  `(doto (Thread. (fn [] ~@body))
     (.start)))

(defmacro threadloop
  [bindings & body]
  `(thread (loop ~bindings ~@body)))

(defn spawn-thread
  [host queue id]
  (let [conn (Jedis. host)]
    (threadloop []
      (.rpush conn queue (into-array String [(str {:user-id id
                                                   :position [(Math/random) (Math/random)]
                                                   :time (time/now)})]))
      (Thread/sleep 1000)
      (recur))))

(defn spawn-threads
  [host queue num]
  (doall (for [i (range 0 num)]
           (spawn-thread host queue i))))

(defn -main
  [& args]
  (spawn-threads "localhost" "bxlqueue" 10)
  (println "Done!"))