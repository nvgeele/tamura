(ns tamura.core-test
  (:use midje.sweet)
  (:require [tamura.core :as core]
            [clojure.core.async :as a :refer [>!! <!!]]
            [multiset.core :as ms]
            [clj-time.core :as t]))

(def ^:dynamic *source-id* nil)
(def ^:dynamic *source-chan* nil)
(def ^:dynamic *test-chan* nil)

(defmacro test-node
  [type timeout node-init & body]
  `(let [source-node# (core/make-source-node ~type :timeout ~timeout)
         init# ~node-init]
     (binding [*source-id* (:id source-node#)
               *source-chan* (:in source-node#)
               *test-chan* (core/chan)]
       (if init#
         (core/node-subscribe (init# source-node#) *test-chan*)
         (core/node-subscribe source-node# *test-chan*))
       ~@body)))

(defmacro test-multiset-node
  [node-init & body]
  `(test-node ::core/multiset false ~node-init ~@body))

(defmacro test-hash-node
  [node-init & body]
  `(test-node ::core/hash false ~node-init ~@body))

(defn send-to-source
  [value]
  (>!! *source-chan* {:destination *source-id* :value value}))

(defn receive-hash
  []
  (:hash (:value (<!! *test-chan*))))

(defn receive-multiset
  []
  (:multiset (:value (<!! *test-chan*))))

;; TODO: test a changed? false send too?

;; TODO: tests for data structures?

;; TODO: tests for delay after buffer?
;; TODO: tests for buffer after delay?

;; TODO: tests for buffer

(facts "about leasing"
  (facts "about leasing multiset source nodes"
    (test-node ::core/multiset
      (t/seconds 10)
      false

      (send-to-source 1)
      (receive-multiset) => (ms/multiset 1)

      (Thread/sleep 5000)

      (send-to-source 2)
      (receive-multiset) => (ms/multiset 1 2)

      (Thread/sleep 6000)

      (send-to-source 3)
      (receive-multiset) => (ms/multiset 2 3)

      (Thread/sleep 11000)

      (send-to-source 4)
      (receive-multiset) => (ms/multiset 4)))
  (facts "about leasing hash source nodes"
    (test-node ::core/hash
      (t/seconds 10)
      false

      (send-to-source [:a 1])
      (receive-hash) => {:a 1}

      (Thread/sleep 5000)

      (send-to-source [:b 1])
      (receive-hash) => {:a 1 :b 1}

      (send-to-source [:a 2])
      (receive-hash) => {:a 2 :b 1}

      (Thread/sleep 6000)

      (send-to-source [:c 1])
      (receive-hash) => {:a 2 :b 1 :c 1}

      (Thread/sleep 11000)

      (send-to-source [:a 1])
      (receive-hash) => {:a 1}))
  (facts "about delay after leased hash source node"
    (test-node ::core/hash
      (t/seconds 10)
      #(core/make-delay-node %)

      (send-to-source [:a 1])
      (receive-hash) => {}

      (send-to-source [:b 1])
      (receive-hash) => {}

      (send-to-source [:a 2])
      (receive-hash) => {:a 1}

      (Thread/sleep 3000)

      (send-to-source [:b 2])
      (receive-hash) => {:a 1 :b 1}

      (Thread/sleep 8000)

      (send-to-source [:c 1])
      (receive-hash) => {:b 1}

      (send-to-source [:a 3])
      (receive-hash) => {:b 1}))
  (facts "about delay after leased multiset source node"
    (test-node ::core/multiset
      (t/seconds 10)
      #(core/make-delay-node %)

      (send-to-source 1)
      (receive-multiset) => (ms/multiset)

      (Thread/sleep 2000)

      (send-to-source 2)
      (receive-multiset) => (ms/multiset 1)

      (send-to-source 3)
      (receive-multiset) => (ms/multiset 1 2)

      (Thread/sleep 9000)

      (send-to-source 4)
      (receive-multiset) => (ms/multiset 2 3)

      (send-to-source 5)
      (receive-multiset) => (ms/multiset 2 3 4))))

(facts "about make-delay-node"
  (facts "about make-delay-node with multisets"
    (test-multiset-node #(core/make-delay-node %)
      (send-to-source 1)
      (receive-multiset) => (ms/multiset)

      (send-to-source 2)
      (receive-multiset) => (ms/multiset 1)

      (send-to-source 3)
      (receive-multiset) => (ms/multiset 1 2)))
  (facts "about make-delay-node with hashes"
    (test-hash-node #(core/make-delay-node %)
      (send-to-source [1 {:v 1}])
      (receive-hash) => {}

      (send-to-source [2 {:v 1}])
      (receive-hash) => {}

      (send-to-source [1 {:v 2}])
      (receive-hash) => {1 {:v 1}}

      (send-to-source [2 {:v 2}])
      (receive-hash) => {1 {:v 1} 2 {:v 1}})))

(facts "about make-multiplicities-node"
  (test-multiset-node #(core/make-multiplicities-node %)
    (send-to-source 'a)
    (receive-multiset) => (ms/multiset ['a 1])

    (send-to-source 'b)
    (receive-multiset) => (ms/multiset ['a 1] ['b 1])

    (send-to-source 'b)
    (receive-multiset) => (ms/multiset ['a 1] ['b 2])))