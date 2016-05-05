(ns examples.lots-of-nodes
  (:require [tamura.core :as t]))

(def n-nodes 128)

(t/defsig source (t/redis "localhost" "masstest"))

(t/defsig last-node (reduce (fn [node n]
                              (t/map-to-multiset identity node))
                            source
                            (range n-nodes)))

(t/print-signal last-node)

;(t/defsig throttled (t/throttle last-node 1000))
;(t/print-signal throttled)

(defn -main [& args]
  (t/start)
  (println "Ready"))