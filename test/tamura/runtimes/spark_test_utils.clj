(ns tamura.runtimes.spark-test-utils
  (:require [flambo.api :as f])
  (:gen-class))

(f/defsparkfn reduce-fn
  [a b]
  (+ a b))

(f/defsparkfn spark-even?
  [n]
  (even? n))

(f/defsparkfn spark-inc
  [n]
  (inc n))

(f/defsparkfn spark-second
  [x]
  (second x))

(f/defsparkfn spark-constant
  [x]
  42)

(f/defsparkfn spark-identity [x] x)