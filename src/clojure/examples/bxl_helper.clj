(ns examples.bxl-helper
  (:gen-class
    :name examples.BxlHelper
    :methods [#^{:static true} [append [Object] void]
              #^{:static true} [getAppended [] java.util.List]
              #^{:static true} [clearAppended [] void]]))


(def max-directions (atom []))

(defn -append
  [o]
  (swap! max-directions conj o)
  nil)

(defn -getAppended
  []
  @max-directions)

(defn -clearAppended
  []
  (reset! max-directions []))