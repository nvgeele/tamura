(ns tamura.util
  (:require [clojure.core.async :as a :refer [>!! >! <!! <! go go-loop]]))

(def buffer-size 32)
(defmacro chan
  ([] `(a/chan ~buffer-size))
  ([size] `(a/chan ~size)))

(defmacro assert-args
  [& pairs]
  `(do (when-not ~(first pairs)
         (throw (IllegalArgumentException.
                  (str (first ~'&form) " requires " ~(second pairs) " in " ~'*ns* ":" (:line (meta ~'&form))))))
       ~(let [more (nnext pairs)]
          (when more
            (list* `assert-args more)))))

(defmacro assert*
  [& pairs]
  (assert-args
    (>= (count pairs) 2) "2 or more expressions in the body"
    (even? (count pairs)) "an even amount of expressions")
  `(do (assert ~(first pairs) ~(second pairs))
       ~(let [more (nnext pairs)]
          (when more
            (list* `assert* more)))))

(defmacro when-let*
  "bindings => [bindingform test ...]"
  [bindings & body]
  (assert-args
    (vector? bindings) "a vector for its binding"
    (>= (count bindings) 2) "2 or more forms in binding vector"
    (even? (count bindings)) "even amount of bindings")
  (let [form (bindings 0) tst (bindings 1)]
    `(let [temp# ~tst]
       (when temp#
         (let [~form temp#
               ~@(rest (rest bindings))]
           ~@body)))))

;; TODO: as macro? Because performance, it's the name of the game
(defn ormap
  [f lst]
  (loop [l lst]
    (cond
      (empty? l) false
      (f (first l)) true
      :else (recur (rest l)))))

(defn andmap
  [f lst]
  (loop [l lst]
    (cond (empty? l) true
          (f (first l)) (recur (rest lst))
          :else false)))