(ns examples.sparkruntime
  (:require [clojure.pprint :refer [pprint]]
            [tamura.core :as t]
            [tamura.config :as cfg])
  (:gen-class))

;; TODO: test buffer diff-add and so on
(defn -main
  [& args]
  (let [r (t/redis "localhost" "q1" :buffer 5)]
    (t/print r)
    ;(t/print (t/diff-add r))
    ;(t/print (t/diff-remove r))

    ;(swap! cfg/config assoc :throttle 2000)
    (swap! cfg/config assoc :runtime :spark)

    (t/start!)
    (println "Ready!")))