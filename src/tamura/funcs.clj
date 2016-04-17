(ns tamura.funcs
  (:require [clojure.core :as core]
            [tamura.values :as v]))

(defn map
  [f lst]
  (if (isa? lst v/Signal)
    (throw (Exception. "ToDo"))
    (core/map f lst)))

(defn lift
  [f]
  (fn [arg]
    (f arg)))