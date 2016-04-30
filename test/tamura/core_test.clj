(ns tamura.core-test
  (:use midje.sweet)
  (:require [tamura.core :as core]
            [clojure.core.async :as a :refer [>!! <!!]]
            [multiset.core :as ms]))

(def ^:dynamic *source-id* nil)
(def ^:dynamic *source-chan* nil)
(def ^:dynamic *test-chan* nil)

(defmacro test-node
  [type node-init & body]
  `(let [source-node# (core/make-source-node ~type)]
     (binding [*source-id* (:id source-node#)
               *source-chan* (:in source-node#)
               *test-chan* (core/chan)]
       (core/node-subscribe (~node-init source-node#) *test-chan*)

       ~@body)))

(defmacro test-multiset-node
  [node-init & body]
  `(test-node ::core/multiset ~node-init ~@body))

(defmacro test-hash-node
  [node-init & body]
  `(test-node ::core/hash ~node-init ~@body))

(defn send-to-source
  [value]
  (>!! *source-chan* {:destination *source-id* :value value}))

(defn receive-hash
  []
  (:hash (:value (<!! *test-chan*))))

(defn receive-multiset
  []
  (:multiset (:value (<!! *test-chan*))))

(facts
  "about make-delay-node"
  (facts
    "about make-delay-node with multisets"
    (test-multiset-node #(core/make-delay-node %)
      ;(send-to-source (core/make-multiset (ms/multiset)))
      ;(receive-multiset) => (ms/multiset)

      (send-to-source 1)
      (receive-multiset) => (ms/multiset)

      (send-to-source 2)
      (receive-multiset) => (ms/multiset 1)

      (send-to-source 3)
      (receive-multiset) => (ms/multiset 1 2)))
  (facts
    "about make-delay-node with hashes"
    (test-hash-node #(core/make-delay-node %)
      ;(send-to-source (core/make-hash {}))
      ;(receive-hash) => {}

      (send-to-source [1 {:v 1}])
      (receive-hash) => {}

      (send-to-source [2 {:v 1}])
      (receive-hash) => {}

      (send-to-source [1 {:v 2}])
      (receive-hash) => {1 {:v 1}}

      (send-to-source [2 {:v 2}])
      (receive-hash) => {1 {:v 1} 2 {:v 1}})))

(facts
  "about make-multiplicities-node"
  (test-multiset-node #(core/make-multiplicities-node %)
    ;(send-to-source (core/make-multiset (ms/multiset)))
    ;(receive-multiset) => (ms/multiset)

    (send-to-source 'a)
    (receive-multiset) => (ms/multiset ['a 1])

    (send-to-source 'b)
    (receive-multiset) => (ms/multiset ['a 1] ['b 1])

    (send-to-source 'b)
    (receive-multiset) => (ms/multiset ['a 1] ['b 2])))