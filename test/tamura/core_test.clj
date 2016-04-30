(ns tamura.core-test
  (:use midje.sweet)
  (:require [tamura.core :as core]
            [clojure.core.async :as a :refer [>!! <!!]]
            [multiset.core :as ms]))

(def ^:dynamic *source-id* nil)
(def ^:dynamic *source-chan* nil)
(def ^:dynamic *test-chan* nil)

(defmacro test-node
  [node-init & body]
  `(let [source-node# (core/make-source-node)]
     (binding [*source-id* (:id source-node#)
               *source-chan* (:in source-node#)
               *test-chan* (core/chan)]
       (core/node-subscribe (~node-init source-node#) *test-chan*)

       ~@body)))

(defn send-to-source
  [value]
  (>!! *source-chan* {:destination *source-id* :value value}))

(facts
  "about make-delay-node"
  (facts
    "about make-delay-node with multisets"
    (test-node #(core/make-delay-node %)
      (send-to-source (core/make-multiset (ms/multiset)))
      (:multiset (:value (<!! *test-chan*))) => (ms/multiset)

      (send-to-source (core/make-multiset (ms/multiset 1)))
      (:multiset (:value (<!! *test-chan*))) => (ms/multiset)

      (send-to-source (core/make-multiset (ms/multiset 1 2)))
      (:multiset (:value (<!! *test-chan*))) => (ms/multiset 1)

      (send-to-source (core/make-multiset (ms/multiset 1 2 3)))
      (:multiset (:value (<!! *test-chan*))) => (ms/multiset 1 2)))

  (facts
    "about make-delay-node with hashes"
    (test-node #(core/make-delay-node %)
      (send-to-source (core/make-hash {}))
      (:hash (:value (<!! *test-chan*))) => {}

      (send-to-source (core/make-hash {1 {:v 1}}))
      (:hash (:value (<!! *test-chan*))) => {}

      (send-to-source (core/make-hash {1 {:v 1} 2 {:v 1}}))
      (:hash (:value (<!! *test-chan*))) => {}

      (send-to-source (core/make-hash {1 {:v 2} 2 {:v 1}}))
      (:hash (:value (<!! *test-chan*))) => {1 {:v 1}}

      (send-to-source (core/make-hash {1 {:v 2} 2 {:v 2}}))
      (:hash (:value (<!! *test-chan*))) => {1 {:v 1} 2 {:v 1}})))