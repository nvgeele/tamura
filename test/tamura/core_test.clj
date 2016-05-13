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

(def test-fns (atom []))

;; NOTE: the node-init function takes the source node for the test as its sole argument
(defmacro test-node
  [type timeout node-init & body]
  `(let [source-id# (core/new-id!)
         source-node# (core/make-source-node source-id# [~type :timeout ~timeout] [])
         init# ~node-init
         test-chan# (core/chan)]
     (if init#
       (core/node-subscribe (init# source-node#) test-chan#)
       (core/node-subscribe source-node# test-chan#))
     (swap! test-fns conj (fn []
                            (binding [*source-id* source-id#
                                      *source-chan* (:in source-node#)
                                      *test-chan* test-chan#
                                      *current-type* ~type]
                              ~@body)))))

(defmacro test-multiset-node
  [node-init & body]
  `(test-node ::core/multiset false ~node-init ~@body))

(defmacro test-hash-node
  [node-init & body]
  `(test-node ::core/hash false ~node-init ~@body))

(defn send
  [value]
  (>!! *source-chan* {:destination *source-id* :value value}))

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

;; TODO: capture test metadata
(defn do-tests
  []
  ;; NOTE: we currently bypass the coordinator, so we do not need to start
  ;; (core/start)
  (doseq [test-fn @test-fns]
    (test-fn)))

;; TODO: test a changed? false send too?
;; TODO: tests for data structures?
;; TODO: change tests so they use the whole static build system thingy

;; TODO: send-receive hybrid

(facts "about simple sources"
  (facts "about multiset simple source"
    (test-multiset-node false
      (send 1)
      (receive) => (ms/multiset 1)

      (send 2)
      (receive) => (ms/multiset 2)

      (send 3)
      (receive) => (ms/multiset 3)))
  (facts "about hash simple source"
    (test-hash-node false
      (send [:a 1])
      (receive) => {:a [1]}

      (send [:b 1])
      (receive) => {:a [1] :b [1]}

      (send [:a 2])
      (receive) => {:a [1 2] :b [1]}

      (send [:a 3])
      (receive) => {:a [1 2 3] :b [1]})))

(comment
  (facts "about leasing"
    (facts "about leasing multiset source nodes"
      (test-node ::core/multiset
        (t/seconds 2)
        false

        (send 1)
        (receive) => (ms/multiset 1)

        (Thread/sleep 1000)

        (send 2)
        (receive) => (ms/multiset 1 2)

        (Thread/sleep 1100)

        (send 3)
        (receive) => (ms/multiset 2 3)

        (Thread/sleep 3000)

        (send 4)
        (receive) => (ms/multiset 4)))
    (facts "about leasing hash source nodes"
      (test-node ::core/hash
        (t/seconds 2)
        false

        (send [:a 1])
        (receive) => {:a 1}

        (Thread/sleep 1000)

        (send [:b 1])
        (receive) => {:a 1 :b 1}

        (send [:a 2])
        (receive) => {:a 2 :b 1}

        (Thread/sleep 1100)

        (send [:c 1])
        (receive) => {:a 2 :b 1 :c 1}

        (Thread/sleep 3000)

        (send [:a 1])
        (receive) => {:a 1}))
    (facts "about delay after leased hash source node"
      (test-node ::core/hash
        (t/seconds 2)
        #(core/make-delay-node (core/new-id!) [] [%])

        (send [:a 1])
        (receive) => {}

        (send [:b 1])
        (receive) => {}

        (send [:a 2])
        (receive) => {:a 1}

        (Thread/sleep 500)

        (send [:b 2])
        (receive) => {:a 1 :b 1}

        (Thread/sleep 1750)

        (send [:c 1])
        (receive) => {:b 1}

        (send [:a 3])
        (receive) => {:b 1}))
    (facts "about delay after leased multiset source node"
      (test-node ::core/multiset
        (t/seconds 2)
        #(core/make-delay-node (core/new-id!) [] [%])

        (send 1)
        (receive) => (ms/multiset)

        (Thread/sleep 500)

        (send 2)
        (receive) => (ms/multiset 1)

        (send 3)
        (receive) => (ms/multiset 1 2)

        (Thread/sleep 1800)

        (send 4)
        (receive) => (ms/multiset 2 3)

        (send 5)
        (receive) => (ms/multiset 2 3 4)))
    (facts "about buffer after leased hash source node"
      (test-node ::core/hash
        (t/seconds 2)
        #(core/make-buffer-node (core/new-id!) [2] [%])

        (send [:a 1])
        (receive) => {:a 1}

        (send [:b 1])
        (receive) => {:a 1 :b 1}

        (send [:c 1])
        (receive) => {:b 1 :c 1}

        (Thread/sleep 3000)

        (send [:d 1])
        (receive) => {:d 1}

        (send [:e 1])
        (receive) => {:d 1 :e 1}))
    (facts "about buffer after leased multiset source node"
      (test-node ::core/multiset
        (t/seconds 2)
        #(core/make-buffer-node (core/new-id!) [2] [%])

        (send 1)
        (receive) => (ms/multiset 1)

        (send 2)
        (receive) => (ms/multiset 1 2)

        (send 3)
        (receive) => (ms/multiset 2 3)

        (Thread/sleep 3000)

        (send 4)
        (receive) => (ms/multiset 4)

        (send 5)
        (receive) => (ms/multiset 4 5))))

  ;; TODO: tests for delay after buffer?
  ;; TODO: tests for buffer after delay?
  (facts "about buffer-node"
    (facts "about multiset buffer-node"
      (test-multiset-node #(core/make-buffer-node (core/new-id!) [3] [%])
        (send 1)
        (receive) => (ms/multiset 1)

        (send 2)
        (receive) => (ms/multiset 1 2)

        (send 3)
        (receive) => (ms/multiset 1 2 3)

        (send 4)
        (receive) => (ms/multiset 2 3 4)

        (send 4)
        (receive) => (ms/multiset 3 4 4)))
    (facts "about hash buffer-node"
      (test-hash-node #(core/make-buffer-node (core/new-id!) [3] [%])
        (send [:a 1])
        (receive) => {:a 1}

        (send [:b 1])
        (receive) => {:a 1 :b 1}

        (send [:c 1])
        (receive) => {:a 1 :b 1 :c 1}

        (send [:d 1])
        (receive) => {:b 1 :c 1 :d 1}

        (send [:b 2])
        (receive) => {:b 2 :c 1 :d 1}

        (send [:e 1])
        (receive) => {:b 2 :d 1 :e 1})))

  (facts "about make-delay-node"
    (facts "about make-delay-node with multisets"
      (test-multiset-node #(core/make-delay-node (core/new-id!) [] [%])
        (send 1)
        (receive) => (ms/multiset)

        (send 2)
        (receive) => (ms/multiset 1)

        (send 3)
        (receive) => (ms/multiset 1 2)))
    (facts "about make-delay-node with hashes"
      (test-hash-node #(core/make-delay-node (core/new-id!) [] [%])
        (send [1 {:v 1}])
        (receive) => {}

        (send [2 {:v 1}])
        (receive) => {}

        (send [1 {:v 2}])
        (receive) => {1 {:v 1}}

        (send [2 {:v 2}])
        (receive) => {1 {:v 1} 2 {:v 1}})))

  (facts "about make-multiplicities-node"
    (test-multiset-node #(core/make-multiplicities-node (core/new-id!) [] [%])
      (send 'a)
      (receive) => (ms/multiset ['a 1])

      (send 'b)
      (receive) => (ms/multiset ['a 1] ['b 1])

      (send 'b)
      (receive) => (ms/multiset ['a 1] ['b 2]))))

;; TODO: perform this test
(comment
  (defn test-sorting
    []
    (register-source! {})                                   ;; 1
    (register-source! {})                                   ;; 2
    (register-source! {})                                   ;; 3
    (register-node! {:inputs [1 2]})                        ;; 4
    (register-node! {:inputs [3 4]})                        ;; 5
    (register-source! {})                                   ;; 6
    (register-node! {:inputs [1 6 5]})                      ;; 7
    (println (sort-nodes))))

(do-tests)