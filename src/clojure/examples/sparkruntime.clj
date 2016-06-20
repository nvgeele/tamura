(ns examples.sparkruntime
  (:require [clojure.pprint :refer [pprint]]
            [tamura.core :as t]
            [tamura.config :as cfg])
  (:gen-class))

;; TODO: test buffer diff-add and so on

;; TODO: test redis throttling and non-throttling

(defn -main
  [& args]

  ;(swap! cfg/config assoc :throttle 1000)
  ;(swap! cfg/config assoc :runtime :spark)

  #_(let [r (t/redis "localhost" "q1" :buffer 5)
        m (t/map r inc)]
    (t/print m)
    (t/redis-out m "localhost" "o1" :flatten? false)
    (t/redis-out m "localhost" "o2" :flatten? true))

  (let [r (t/redis "localhost" "q2" :key :id)
        m (t/map-by-key r (comp inc :v))]
    (t/print m)
    (t/redis-out m "localhost" "o1" :flatten? false)
    (t/redis-out m "localhost" "o2" :flatten? true))

  (t/start!)
  (println "Ready!"))