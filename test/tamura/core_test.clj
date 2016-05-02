(ns tamura.core-test
  (:use midje.sweet)
  (:require [tamura.core :as core]
            [clojure.core.async :as a :refer [>!! <!!]]
            [multiset.core :as ms]
            [clj-time.core :as t]))

(def ^:dynamic *source-id* nil)
(def ^:dynamic *source-chan* nil)
(def ^:dynamic *test-chan* nil)
(def ^:dynamic *current-type* nil)

(defmacro test-node
  [type timeout node-init & body]
  `(let [source-node# (core/make-source-node ~type :timeout ~timeout)
         init# ~node-init]
     (binding [*source-id* (:id source-node#)
               *source-chan* (:in source-node#)
               *test-chan* (core/chan)
               *current-type* ~type]
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

;; TODO: rename to send
(defn send-to-source
  [value]
  (>!! *source-chan* {:destination *source-id* :value value}))

(def send send-to-source)

(defn receive-hash
  []
  (:hash (:value (<!! *test-chan*))))

(defn receive-multiset
  []
  (:multiset (:value (<!! *test-chan*))))

(defn receive
  []
  (case *current-type*
    ::core/multiset (receive-multiset)
    ::core/hash (receive-hash)
    (throw (Exception. "*current-type* not bound"))))

;; TODO: test a changed? false send too?

;; TODO: tests for data structures?

(facts "about leasing"
  (facts "about leasing multiset source nodes"
    (test-node ::core/multiset
      (t/seconds 10)
      false

      (send-to-source 1)
      (receive) => (ms/multiset 1)

      (Thread/sleep 5000)

      (send-to-source 2)
      (receive) => (ms/multiset 1 2)

      (Thread/sleep 6000)

      (send-to-source 3)
      (receive) => (ms/multiset 2 3)

      (Thread/sleep 11000)

      (send-to-source 4)
      (receive) => (ms/multiset 4)))
  (facts "about leasing hash source nodes"
    (test-node ::core/hash
      (t/seconds 10)
      false

      (send-to-source [:a 1])
      (receive) => {:a 1}

      (Thread/sleep 5000)

      (send-to-source [:b 1])
      (receive) => {:a 1 :b 1}

      (send-to-source [:a 2])
      (receive) => {:a 2 :b 1}

      (Thread/sleep 6000)

      (send-to-source [:c 1])
      (receive) => {:a 2 :b 1 :c 1}

      (Thread/sleep 11000)

      (send-to-source [:a 1])
      (receive) => {:a 1}))
  (facts "about delay after leased hash source node"
    (test-node ::core/hash
      (t/seconds 10)
      #(core/make-delay-node %)

      (send-to-source [:a 1])
      (receive) => {}

      (send-to-source [:b 1])
      (receive) => {}

      (send-to-source [:a 2])
      (receive) => {:a 1}

      (Thread/sleep 3000)

      (send-to-source [:b 2])
      (receive) => {:a 1 :b 1}

      (Thread/sleep 8000)

      (send-to-source [:c 1])
      (receive) => {:b 1}

      (send-to-source [:a 3])
      (receive) => {:b 1}))
  (facts "about delay after leased multiset source node"
    (test-node ::core/multiset
      (t/seconds 10)
      #(core/make-delay-node %)

      (send-to-source 1)
      (receive) => (ms/multiset)

      (Thread/sleep 2000)

      (send-to-source 2)
      (receive) => (ms/multiset 1)

      (send-to-source 3)
      (receive) => (ms/multiset 1 2)

      (Thread/sleep 9000)

      (send-to-source 4)
      (receive) => (ms/multiset 2 3)

      (send-to-source 5)
      (receive) => (ms/multiset 2 3 4)))
  (facts "about buffer after leased hash source node"
    (test-node ::core/hash
      (t/seconds 10)
      #(core/make-buffer-node % 2)

      (send [:a 1])
      (receive) => {:a 1}

      (send [:b 1])
      (receive) => {:a 1 :b 1}

      (send [:c 1])
      (receive) => {:b 1 :c 1}

      (Thread/sleep 11000)

      (send [:d 1])
      (receive) => {:d 1}

      (send [:e 1])
      (receive) => {:d 1 :e 1}))
  (facts "about buffer after leased multiset source node"
    (test-node ::core/multiset
      (t/seconds 10)
      #(core/make-buffer-node % 2)

      (send-to-source 1)
      (receive) => (ms/multiset 1)

      (send-to-source 2)
      (receive) => (ms/multiset 1 2)

      (send-to-source 3)
      (receive) => (ms/multiset 2 3)

      (Thread/sleep 11000)

      (send-to-source 4)
      (receive) => (ms/multiset 4)

      (send-to-source 5)
      (receive) => (ms/multiset 4 5))))

;; TODO: tests for delay after buffer?
;; TODO: tests for buffer after delay?
(facts "about buffer-node"
  (facts "about multiset buffer-node"
    (test-multiset-node #(core/make-buffer-node % 3)
      (send-to-source 1)
      (receive) => (ms/multiset 1)

      (send-to-source 2)
      (receive) => (ms/multiset 1 2)

      (send-to-source 3)
      (receive) => (ms/multiset 1 2 3)

      (send-to-source 4)
      (receive) => (ms/multiset 2 3 4)

      (send-to-source 4)
      (receive) => (ms/multiset 3 4 4)))
  (facts "about hash buffer-node"
    (test-hash-node #(core/make-buffer-node % 3)
      (send-to-source [:a 1])
      (receive) => {:a 1}

      (send-to-source [:b 1])
      (receive) => {:a 1 :b 1}

      (send-to-source [:c 1])
      (receive) => {:a 1 :b 1 :c 1}

      (send-to-source [:d 1])
      (receive) => {:b 1 :c 1 :d 1}

      (send-to-source [:b 2])
      (receive) => {:b 2 :c 1 :d 1}

      (send-to-source [:e 1])
      (receive) => {:b 2 :d 1 :e 1})))

(facts "about make-delay-node"
  (facts "about make-delay-node with multisets"
    (test-multiset-node #(core/make-delay-node %)
      (send-to-source 1)
      (receive) => (ms/multiset)

      (send-to-source 2)
      (receive) => (ms/multiset 1)

      (send-to-source 3)
      (receive) => (ms/multiset 1 2)))
  (facts "about make-delay-node with hashes"
    (test-hash-node #(core/make-delay-node %)
      (send-to-source [1 {:v 1}])
      (receive) => {}

      (send-to-source [2 {:v 1}])
      (receive) => {}

      (send-to-source [1 {:v 2}])
      (receive) => {1 {:v 1}}

      (send-to-source [2 {:v 2}])
      (receive) => {1 {:v 1} 2 {:v 1}})))

(facts "about make-multiplicities-node"
  (test-multiset-node #(core/make-multiplicities-node %)
    (send-to-source 'a)
    (receive) => (ms/multiset ['a 1])

    (send-to-source 'b)
    (receive) => (ms/multiset ['a 1] ['b 1])

    (send-to-source 'b)
    (receive) => (ms/multiset ['a 1] ['b 2])))