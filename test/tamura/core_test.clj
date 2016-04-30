(ns tamura.core-test
  (:use midje.sweet)
  (:require [tamura.core :as core]
            [clojure.core.async :as a :refer [>!! <!!]]
            [multiset.core :as ms]))

(facts
  "about make-delay-node"
  (facts
    "about make-delay-node with multisets"
    (let [source-node (core/make-source-node)
          delay-node (core/make-delay-node source-node)
          test-chan (core/chan)
          source-chan (:in source-node)
          id (:id source-node)]

      (core/node-subscribe delay-node test-chan)

      (>!! source-chan {:destination id :value (core/make-multiset (ms/multiset))})
      (:multiset (:value (<!! test-chan))) => (ms/multiset)

      (>!! source-chan {:destination id :value (core/make-multiset (ms/multiset 1))})
      (:multiset (:value (<!! test-chan))) => (ms/multiset)

      (>!! source-chan {:destination id :value (core/make-multiset (ms/multiset 1 2))})
      (:multiset (:value (<!! test-chan))) => (ms/multiset 1)

      (>!! source-chan {:destination id :value (core/make-multiset (ms/multiset 1 2 3))})
      (:multiset (:value (<!! test-chan))) => (ms/multiset 1 2)))
  (facts
    "about make-delay-node with hashes"
    (let [source-node (core/make-source-node)
          delay-node (core/make-delay-node source-node)
          test-chan (core/chan)
          source-chan (:in source-node)
          id (:id source-node)]

      (core/node-subscribe delay-node test-chan)

      (>!! source-chan {:destination id :value (core/make-hash {})})
      (:hash (:value (<!! test-chan))) => {}

      (>!! source-chan {:destination id :value (core/make-hash {1 {:v 1}})})
      (:hash (:value (<!! test-chan))) => {}

      (>!! source-chan {:destination id :value (core/make-hash {1 {:v 1} 2 {:v 1}})})
      (:hash (:value (<!! test-chan))) => {}

      (>!! source-chan {:destination id :value (core/make-hash {1 {:v 2} 2 {:v 1}})})
      (:hash (:value (<!! test-chan))) => {1 {:v 1}}

      (>!! source-chan {:destination id :value (core/make-hash {1 {:v 2} 2 {:v 2}})})
      (:hash (:value (<!! test-chan))) => {1 {:v 1} 2 {:v 1}})))