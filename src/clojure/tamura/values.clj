(ns tamura.values)

(defrecord Signal [value])

(defn signal?
  [x]
  (instance? Signal x))
(defn make-signal
  [node]
  (Signal. node))
(defn signal-value
  [signal]
  (:value signal))