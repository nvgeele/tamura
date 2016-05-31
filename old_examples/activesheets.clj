(ns examples.activesheets
  (:require [tamura.core :as t]))

(def prices {:child 0
             :student 10
             :senior 10
             :regular 15})

(t/defsig input (set-of-person-kind-tuples))
(t/defsig output (t/map (fn [[person kind]] [person (get prices kind)]) input))

(t/defsig input (set-of-seqNo-travelTime-tuples))


(defn -main
  [& args]
  (println "fuck"))