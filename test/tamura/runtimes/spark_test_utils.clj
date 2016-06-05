(ns tamura.runtimes.spark-test-utils
  (:require [flambo.api :as f])
  (:gen-class))

(f/defsparkfn reduce-fn
  [a b]
  (+ a b))

(f/defsparkfn spark-even?
  [n]
  (even? n))