(ns tamura.core-test
  (:use midje.sweet
        tamura.datastructures)
  (:require [clojure.core.async :as a :refer [>!! <!!]]
            [clj-time.core :as t]
            [multiset.core :as ms]
            [tamura.core :as core]
            [tamura.util :refer [chan new-id!]]
            [tamura.node :as n]))

(def ^:dynamic *source-id* nil)
(def ^:dynamic *source-chan* nil)
(def ^:dynamic *test-chan* nil)
(def ^:dynamic *input-type* nil)
(def ^:dynamic *return-type* nil)

(def test-fns (atom []))

;; NOTE: the node-init function takes the source node for the test as its sole argument
;; TODO: buffer!
;; TODO: use return-type of node to set the current-type
(defmacro test-node
  [input-type timeout buffer node-init & body]
  `(let [source-id# (new-id!)
         source-node# (core/make-source-node source-id# [~input-type :timeout ~timeout :buffer ~buffer] [])
         init# ~node-init
         test-chan# (chan)
         out-node# (if init#
                     (init# source-node#)
                     source-node#)]
     (n/node-subscribe out-node# test-chan#)
     (swap! test-fns conj (fn []
                            (binding [*source-id* source-id#
                                      *source-chan* (:in source-node#)
                                      *test-chan* test-chan#
                                      *input-type* ~input-type
                                      *return-type* (:return-type out-node#)]
                              ~@body)))))

(defmacro test-multiset-node
  [node-init & body]
  `(test-node :multiset false false ~node-init ~@body))

(defmacro test-hash-node
  [node-init & body]
  `(test-node :hash false false ~node-init ~@body))

(defn send
  ([value]
   (>!! *source-chan* {:destination *source-id* :value value}))
  ([key val]
    (send [key val])))

(defn receive-hash
  []
  (to-hash (:value (<!! *test-chan*))))

(defn receive-multiset
  []
  (to-multiset (:value (<!! *test-chan*))))

(defn receive
  []
  (case *return-type*
    :multiset (receive-multiset)
    :hash (receive-hash)
    (throw (Exception. "*return-type* not bound"))))

(defn send-receive
  ([value]
   (send value)
   (receive))
  ([key val]
   (send-receive [key val])))

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
(facts "about simple sources"
  (facts "multiset"
    (test-multiset-node false
      (send-receive 1) => (ms/multiset 1)
      (send-receive 2) => (ms/multiset 1 2)
      (send-receive 3) => (ms/multiset 1 2 3)))
  (facts "hash"
    (test-hash-node false
      (send-receive :a 1) => {:a (ms/multiset 1)}
      (send-receive :b 1) => {:a (ms/multiset 1) :b (ms/multiset 1)}
      (send-receive :a 2) => {:a (ms/multiset 1 2) :b (ms/multiset 1)}
      (send-receive :a 3) => {:a (ms/multiset 1 2 3) :b (ms/multiset 1)})))

(facts "about buffered source nodes"
  (facts "multiset"
    (test-node :multiset false 2 false
      (send-receive 1) => (ms/multiset 1)
      (send-receive 2) => (ms/multiset 1 2)
      (send-receive 3) => (ms/multiset 2 3)))
  (facts "hash"
    (test-node :hash false 2 false
      (send-receive :a 1) => {:a (ms/multiset 1)}
      (send-receive :b 1) => {:a (ms/multiset 1) :b (ms/multiset 1)}
      (send-receive :c 1) => {:a (ms/multiset 1) :b (ms/multiset 1) :c (ms/multiset 1)}
      (send-receive :a 2) => {:a (ms/multiset 1 2) :b (ms/multiset 1) :c (ms/multiset 1)}
      (send-receive :a 3) => {:a (ms/multiset 2 3) :b (ms/multiset 1) :c (ms/multiset 1)})))

(facts "about time-based leasing"
  (facts "multiset"
    (test-node :multiset (t/seconds 2) false false
      (send-receive 1) => (ms/multiset 1)
      (Thread/sleep 1000)
      (send-receive 2) => (ms/multiset 1 2)
      (Thread/sleep 1100)
      (send-receive 3) => (ms/multiset 2 3)
      (Thread/sleep 3000)
      (send-receive 4) => (ms/multiset 4)))
  (facts "hash"
    (test-node :hash (t/seconds 2) false false
      (send-receive :a 1) => {:a (ms/multiset 1)}
      (send-receive :a 2) => {:a (ms/multiset 1 2)}
      (Thread/sleep 1000)
      (send-receive :a 3) => {:a (ms/multiset 1 2 3)}
      (Thread/sleep 1100)
      (send-receive :b 1) => {:a (ms/multiset 3) :b (ms/multiset 1)})))

(facts "about time-based, buffered leasing"
  (facts "multiset"
    (test-node :multiset (t/seconds 2) 2 false
      (send-receive 1) => (ms/multiset 1)
      (send-receive 2) => (ms/multiset 1 2)
      (Thread/sleep 1000)
      (send-receive 3) => (ms/multiset 2 3)
      (Thread/sleep 1100)
      (send-receive 4) => (ms/multiset 3 4)))
  (facts "hash"
    (test-node :hash (t/seconds 2) 2 false
      (send-receive :a 1) => {:a (ms/multiset 1)}
      (send-receive :a 2) => {:a (ms/multiset 1 2)}
      (Thread/sleep 1000)
      (send-receive :a 3) => {:a (ms/multiset 2 3)}
      (Thread/sleep 1100)
      (send-receive :b 1) => {:a (ms/multiset 3) :b (ms/multiset 1)})))

(facts "about delay"
  (facts "multiset"
    (test-multiset-node #(core/make-delay-node (new-id!) [] [%])
      (send-receive 1) => (ms/multiset)
      (send-receive 2) => (ms/multiset 1)
      (send-receive 3) => (ms/multiset 1 2)))
  (facts "hash"
    (test-hash-node #(core/make-delay-node (new-id!) [] [%])
      (send-receive :a 1) => {}
      (send-receive :b 1) => {:a (ms/multiset 1)}
      (send-receive :a 2) => {:a (ms/multiset 1) :b (ms/multiset 1)}))
  (facts "about delay after leased and buffered node"
    (facts "multiset (buffer size 3)"
      (test-node :multiset (t/seconds 2) 3 #(core/make-delay-node (new-id!) [] [%])
        (send-receive 1) => (ms/multiset)
        (send-receive 2) => (ms/multiset 1)
        (send-receive 3) => (ms/multiset 1 2)
        (send-receive 4) => (ms/multiset 1 2 3)
        (send-receive 5) => (ms/multiset 2 3 4)
        (Thread/sleep 2100)
        (send-receive 6) => (ms/multiset 3 4 5)
        (send-receive 7) => (ms/multiset 6)))
    (facts "hash (buffer size 2)"
      (test-node :hash (t/seconds 2) 2 #(core/make-delay-node (new-id!) [] [%])
        (send-receive :a 1) => {}
        (send-receive :a 2) => {:a (ms/multiset 1)}
        (Thread/sleep 1000)
        (send-receive :b 1) => {:a (ms/multiset 1 2)}
        (send-receive :b 2) => {:a (ms/multiset 1 2) :b (ms/multiset 1)}
        (Thread/sleep 1100)
        (send-receive :c 1) => {:a (ms/multiset 1 2) :b (ms/multiset 1 2)}
        (send-receive :a 3) => {:b (ms/multiset 1 2) :c (ms/multiset 1)}
        (send-receive :b 3) => {:b (ms/multiset 1 2) :c (ms/multiset 1) :a (ms/multiset 3)}))))

;; TODO: tests for delay after buffer?
;; TODO: tests for buffer after delay?
(facts "about buffer"
  (facts "multiset"
    (test-multiset-node #(core/make-buffer-node (new-id!) [2] [%])
      (send-receive 1) => (ms/multiset 1)
      (send-receive 2) => (ms/multiset 1 2)
      (send-receive 3) => (ms/multiset 2 3)
      (send-receive 4) => (ms/multiset 3 4)))
  (facts "hash"
    (test-hash-node #(core/make-buffer-node (new-id!) [2] [%])
      (send-receive :a 1) => {:a (ms/multiset 1)}
      (send-receive :a 2) => {:a (ms/multiset 1 2)}
      (send-receive :a 3) => {:a (ms/multiset 2 3)}
      (send-receive :a 4) => {:a (ms/multiset 3 4)}))
  (facts "about buffer after leased and buffered node"
    (facts "multiset (source buffer size 3)"
      (test-node :multiset (t/seconds 2) 3 #(core/make-buffer-node (new-id!) [2] [%])
        (send-receive 1) => (ms/multiset 1)
        (send-receive 2) => (ms/multiset 1 2)
        (send-receive 3) => (ms/multiset 2 3)
        (Thread/sleep 2100)
        (send-receive 4) => (ms/multiset 4)
        (send-receive 5) => (ms/multiset 4 5)))
    (facts "multiset, buffer of size 3 after buffered source of size 2"
      (test-node :multiset (t/seconds 2) 2 #(core/make-buffer-node (new-id!) [3] [%])
        (send-receive 1) => (ms/multiset 1)
        (send-receive 2) => (ms/multiset 1 2)
        (send-receive 3) => (ms/multiset 2 3)
        (send-receive 4) => (ms/multiset 3 4)))
    (facts "hash (source buffer size 3)"
      (test-node :hash (t/seconds 2) 3 #(core/make-buffer-node (new-id!) [2] [%])
        (send-receive :a 1) => {:a (ms/multiset 1)}
        (send-receive :a 2) => {:a (ms/multiset 1 2)}
        (send-receive :a 3) => {:a (ms/multiset 2 3)}
        (send-receive :b 1) => {:a (ms/multiset 2 3) :b (ms/multiset 1)}
        (Thread/sleep 2100)
        (send-receive :d 1) => {:d (ms/multiset 1)}
        (send-receive :e 1) => {:d (ms/multiset 1) :e (ms/multiset 1)}))))

(facts "about diff-add"
  (facts "multiset after source (timed)"
    (test-node :multiset (t/seconds 2) false #(core/make-diff-add-node (new-id!) [] [%])
      (send-receive 1) => (ms/multiset 1)
      (send-receive 2) => (ms/multiset 2)
      (send-receive 3) => (ms/multiset 3)
      (Thread/sleep 2100)
      (send-receive 4) => (ms/multiset 4)))
  (facts "hash after source (timed)"
    (test-node :hash (t/seconds 2) false #(core/make-diff-add-node (new-id!) [] [%])
      (send-receive :a 1) => (ms/multiset [:a 1])
      (send-receive :b 1) => (ms/multiset [:b 1])
      (Thread/sleep 2100)
      (send-receive :c 1) => (ms/multiset [:c 1])))
  (facts "multiset after buffer (size 2)"
    (test-node :multiset false false
      #(let [buffer-node (core/make-buffer-node (new-id!) [2] [%])]
        (core/make-diff-add-node (new-id!) [] [buffer-node]))
      (send-receive 1) => (ms/multiset 1)
      (send-receive 2) => (ms/multiset 2)
      (send-receive 3) => (ms/multiset 3)))
  (facts "hash after buffer (size 2)"
    (test-node :hash false false
      #(let [buffer-node (core/make-buffer-node (new-id!) [2] [%])]
        (core/make-diff-add-node (new-id!) [] [buffer-node]))
      (send-receive :a 1) => (ms/multiset [:a 1])
      (send-receive :a 2) => (ms/multiset [:a 2])
      (send-receive :a 3) => (ms/multiset [:a 3]))))

(facts "about diff-remove"
  (facts "multiset after source (timed)"
    (test-node :multiset (t/seconds 2) false #(core/make-diff-remove-node (new-id!) [] [%])
      (send-receive 1) => (ms/multiset)
      (send-receive 2) => (ms/multiset)
      (send-receive 3) => (ms/multiset)
      (Thread/sleep 2100)
      (send-receive 4) => (ms/multiset 1 2 3)))
  (facts "hash after source (timed)"
    (test-node :hash (t/seconds 2) false #(core/make-diff-remove-node (new-id!) [] [%])
      (send-receive :a 1) => (ms/multiset)
      (send-receive :b 1) => (ms/multiset)
      (Thread/sleep 2100)
      (send-receive :c 1) => (ms/multiset [:a 1] [:b 1])))
  (facts "multiset after buffer (size 2)"
    (test-node :multiset false false
      #(let [buffer-node (core/make-buffer-node (new-id!) [2] [%])]
        (core/make-diff-remove-node (new-id!) [] [buffer-node]))
      (send-receive 1) => (ms/multiset)
      (send-receive 2) => (ms/multiset)
      (send-receive 3) => (ms/multiset 1)))
  (facts "hash after buffer (size 2)"
    (test-node :hash false false
      #(let [buffer-node (core/make-buffer-node (new-id!) [2] [%])]
        (core/make-diff-remove-node (new-id!) [] [buffer-node]))
      (send-receive :a 1) => (ms/multiset)
      (send-receive :a 2) => (ms/multiset)
      (send-receive :a 3) => (ms/multiset [:a 1]))))

(facts "about make-multiplicities-node"
  (test-multiset-node #(core/make-multiplicities-node (new-id!) [] [%])
    (send-receive 'a) => (ms/multiset ['a 1])
    (send-receive 'b) => (ms/multiset ['a 1] ['b 1])
    (send-receive 'b) => (ms/multiset ['a 1] ['b 2])))

(facts "about filter-key-size node (size 2)"
  (test-hash-node #(core/make-filter-key-size-node (new-id!) [2] [%])
    (send-receive :a 1) => {}
    (send-receive :b 1) => {}
    (send-receive :a 2) => {:a (ms/multiset 1 2)}
    (send-receive :b 2) => {:a (ms/multiset 1 2) :b (ms/multiset 1 2)}
    (send-receive :b 3) => {:a (ms/multiset 1 2) :b (ms/multiset 1 2 3)}))

(facts "about reduce, function (fn [a b] (+ a b))"
  (facts "multiset (no initial)"
    (test-multiset-node #(core/make-reduce-node (new-id!) [(fn [a b] (+ a b)) false] [%])
      (send-receive 1) => (ms/multiset 1)
      (send-receive 2) => (ms/multiset 3)
      (send-receive 3) => (ms/multiset 6)))
  (facts "multiset (initial = -1)"
    (test-multiset-node #(core/make-reduce-node (new-id!) [(fn [a b] (+ a b)) {:val -1}] [%])
      (send-receive 1) => (ms/multiset 0)
      (send-receive 2) => (ms/multiset 2)
      (send-receive 3) => (ms/multiset 5))))

(facts "about reduce-by-key, function (fn [a b] (+ a b))"
  (facts "hash (no initial)"
    (test-hash-node #(core/make-reduce-by-key-node (new-id!) [(fn [a b] (+ a b)) false] [%])
      (send-receive :a 1) => {:a (ms/multiset 1)}
      (send-receive :a 2) => {:a (ms/multiset 3)}
      (send-receive :a 3) => {:a (ms/multiset 6)}
      (send-receive :b 1) => {:a (ms/multiset 6) :b (ms/multiset 1)}
      (send-receive :b 2) => {:a (ms/multiset 6) :b (ms/multiset 3)}))
  (facts "hash (initial = -1)"
    (test-hash-node #(core/make-reduce-by-key-node (new-id!) [(fn [a b] (+ a b)) {:val -1}] [%])
      (send-receive :a 1) => {:a (ms/multiset 0)}
      (send-receive :a 2) => {:a (ms/multiset 2)}
      (send-receive :a 3) => {:a (ms/multiset 5)}
      (send-receive :b 1) => {:a (ms/multiset 5) :b (ms/multiset 0)}
      (send-receive :b 2) => {:a (ms/multiset 5) :b (ms/multiset 2)})))

(facts "about hash-to-multiset"
  (test-hash-node #(core/make-hash-to-multiset-node (new-id!) [] [%])
    (send-receive :a 1) => (ms/multiset [:a 1])
    (send-receive :b 1) => (ms/multiset [:a 1] [:b 1])
    (send-receive :b 2) => (ms/multiset [:a 1] [:b 1] [:b 2])))

(facts "about map, function inc"
  (test-multiset-node #(core/make-map-node (new-id!) [inc] [%])
    (send-receive 1) => (ms/multiset 2)
    (send-receive 2) => (ms/multiset 2 3)
    (send-receive 3) => (ms/multiset 2 3 4)))

(facts "about map-by-key, function inc"
  (test-hash-node #(core/make-map-by-key-node (new-id!) [inc] [%])
    (send-receive :a 1) => {:a (ms/multiset 2)}
    (send-receive :b 1) => {:a (ms/multiset 2) :b (ms/multiset 2)}
    (send-receive :b 2) => {:a (ms/multiset 2) :b (ms/multiset 2 3)}))

(facts "about filter, function even?"
  (test-multiset-node #(core/make-filter-node (new-id!) [even?] [%])
    (send-receive 1) => (ms/multiset)
    (send-receive 2) => (ms/multiset 2)
    (send-receive 3) => (ms/multiset 2)))

(facts "about filter-by-key, function even?"
  (test-hash-node #(core/make-filter-by-key-node (new-id!) [even?] [%])
    (send-receive :a 1) => {}
    (send-receive :b 1) => {}
    (send-receive :a 2) => {:a (ms/multiset 2)}
    (send-receive :b 2) => {:a (ms/multiset 2) :b (ms/multiset 2)}))

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