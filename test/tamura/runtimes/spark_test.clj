(ns tamura.runtimes.spark-test
  (:refer-clojure :exclude [send])
  (:use [midje.sweet :exclude [facts]]
        tamura.datastructures
        tamura.runtimes.spark-test-utils)
  (:require [clojure.core.async :as a :refer [>!! <!!]]
            [clj-time.core :as t]
            [multiset.core :as ms]
            [tamura.node :as n]
            [tamura.runtimes.spark :as spark]
            [tamura.util :refer [chan new-id!]]
            [midje.sweet]))

(def ^:dynamic *source-id* nil)
(def ^:dynamic *source-chan* nil)
(def ^:dynamic *test-chan* nil)
(def ^:dynamic *input-type* nil)
(def ^:dynamic *return-type* nil)
(def ^:dynamic *current-test* nil)

(def ^:dynamic *source-id2* nil)
(def ^:dynamic *source-chan2* nil)

(def test-fns (atom []))

(defmacro facts
  [& body]
  `(let [ts# ~(first body)
         bs# (if *current-test*
               (str *current-test* " > " ts#)
               ts#)]
     (binding [*current-test* bs#]
       (midje.sweet/facts ts#
         ~@(rest body)))))

;; NOTE: the node-init function takes the source node for the test as its sole argument
;; TODO: buffer!
;; TODO: use return-type of node to set the current-type
(defmacro test-node
  [input-type timeout buffer node-init & body]
  `(let [ct# *current-test*]
     (swap! test-fns
            conj
            (fn []
              (print (str \u001b "[32m" ct# " ... " \u001b "[0m")) (flush)
              (let [source-id# (new-id!)
                    source-node# (spark/make-source-node source-id# [~input-type :timeout ~timeout :buffer ~buffer] [])
                    init# ~node-init
                    test-chan# (chan)
                    out-node# (if init#
                                (init# source-node#)
                                source-node#)]
                (n/node-subscribe out-node# test-chan#)
                ;; 100 ms sleep to make sure nodes are subscribed and so on
                (Thread/sleep 100)
                (binding [*source-id* source-id#
                          *source-chan* (:in source-node#)
                          *test-chan* test-chan#
                          *input-type* ~input-type
                          *return-type* (:return-type out-node#)]
                  ~@body))
              (println (str \u001b "[32mdone" \u001b "[0m"))))))

;; I know this approach is a bit messy...
(defmacro test-binode
  [input-type node-init & body]
  `(let [ct# *current-test*]
     (swap! test-fns
            conj
            (fn []
              (print (str \u001b "[32m" ct# " ... " \u001b "[0m")) (flush)
              (let [source-id1# (new-id!)
                    source-id2# (new-id!)
                    source-node1# (spark/make-source-node source-id1# [~input-type] [])
                    source-node2# (spark/make-source-node source-id2# [~input-type] [])
                    init# ~node-init
                    test-chan# (chan)
                    out-node# (init# source-node1# source-node2#)]
                (n/node-subscribe out-node# test-chan#)
                ;; 100 ms sleep to make sure nodes are subscribed and so on
                (Thread/sleep 100)
                (binding [*source-id* source-id1#
                          *source-id2* source-id2#
                          *source-chan* (:in source-node1#)
                          *source-chan2* (:in source-node2#)
                          *test-chan* test-chan#
                          *input-type* ~input-type
                          *return-type* (:return-type out-node#)]
                  ~@body))
              (println (str \u001b "[32mdone" \u001b "[0m"))
              ))))

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
   (>!! *source-chan* {:destination *source-id* :value [key val]})))

(defn receive-hash
  []
  (spark/collect-hash* (:value (<!! *test-chan*))))

(defn receive-multiset
  []
  (spark/collect-multiset* (:value (<!! *test-chan*))))

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

(defn bi-send
  [value source]
  (let [msg {:destination (if (= source 1)
                            *source-id*
                            *source-id2*)
             :value value}]
    (>!! *source-chan* msg)
    (>!! *source-chan2* msg)))

(defn bi-send-receive
  ([value source]
   (bi-send value source)
   (receive))
  ([key val source]
   (bi-send-receive [key val] source)))

;; TODO: capture test metadata
;; TODO: set-up spark
(defn do-tests
  []
  ;; NOTE: we currently bypass the coordinator, so we do not need to start
  (spark/setup-spark!)
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

;; polymorphic
(comment
  (facts "about delay"
    (facts "multiset"
      (test-multiset-node #(spark/make-delay-node (new-id!) [] [%])
        (send-receive 1) => (ms/multiset)
        (send-receive 2) => (ms/multiset 1)
        (send-receive 3) => (ms/multiset 1 2)))
    (facts "hash"
      (test-hash-node #(spark/make-delay-node (new-id!) [] [%])
        (send-receive :a 1) => {}
        (send-receive :b 1) => {:a (ms/multiset 1)}
        (send-receive :a 2) => {:a (ms/multiset 1) :b (ms/multiset 1)}))
    (facts "about delay after leased and buffered node"
      (facts "multiset (buffer size 3)"
        (test-node :multiset (t/seconds 2) 3 #(spark/make-delay-node (new-id!) [] [%])
          (send-receive 1) => (ms/multiset)
          (send-receive 2) => (ms/multiset 1)
          (send-receive 3) => (ms/multiset 1 2)
          (send-receive 4) => (ms/multiset 1 2 3)
          (send-receive 5) => (ms/multiset 2 3 4)
          (Thread/sleep 2100)
          (send-receive 6) => (ms/multiset 3 4 5)
          (send-receive 7) => (ms/multiset 6)))
      (facts "hash (buffer size 2)"
        (test-node :hash (t/seconds 2) 2 #(spark/make-delay-node (new-id!) [] [%])
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
      (test-multiset-node #(spark/make-buffer-node (new-id!) [2] [%])
        (send-receive 1) => (ms/multiset 1)
        (send-receive 2) => (ms/multiset 1 2)
        (send-receive 3) => (ms/multiset 2 3)
        (send-receive 4) => (ms/multiset 3 4)))
    (facts "hash"
      (test-hash-node #(spark/make-buffer-node (new-id!) [2] [%])
        (send-receive :a 1) => {:a (ms/multiset 1)}
        (send-receive :a 2) => {:a (ms/multiset 1 2)}
        (send-receive :a 3) => {:a (ms/multiset 2 3)}
        (send-receive :a 4) => {:a (ms/multiset 3 4)}))
    (facts "about buffer after leased and buffered node"
      (facts "multiset (source buffer size 3)"
        (test-node :multiset (t/seconds 2) 3 #(spark/make-buffer-node (new-id!) [2] [%])
          (send-receive 1) => (ms/multiset 1)
          (send-receive 2) => (ms/multiset 1 2)
          (send-receive 3) => (ms/multiset 2 3)
          (Thread/sleep 2100)
          (send-receive 4) => (ms/multiset 4)
          (send-receive 5) => (ms/multiset 4 5)))
      (facts "multiset, buffer of size 3 after buffered source of size 2"
        (test-node :multiset (t/seconds 2) 2 #(spark/make-buffer-node (new-id!) [3] [%])
          (send-receive 1) => (ms/multiset 1)
          (send-receive 2) => (ms/multiset 1 2)
          (send-receive 3) => (ms/multiset 2 3)
          (send-receive 4) => (ms/multiset 3 4)))
      (facts "hash (source buffer size 3)"
        (test-node :hash (t/seconds 2) 3 #(spark/make-buffer-node (new-id!) [2] [%])
          (send-receive :a 1) => {:a (ms/multiset 1)}
          (send-receive :a 2) => {:a (ms/multiset 1 2)}
          (send-receive :a 3) => {:a (ms/multiset 2 3)}
          (send-receive :b 1) => {:a (ms/multiset 2 3) :b (ms/multiset 1)}
          (Thread/sleep 2100)
          (send-receive :d 1) => {:d (ms/multiset 1)}
          (send-receive :e 1) => {:d (ms/multiset 1) :e (ms/multiset 1)}))))

  (facts "about diff-add"
    (facts "multiset after source (timed)"
      (test-node :multiset (t/seconds 2) false #(spark/make-diff-add-node (new-id!) [] [%])
        (send-receive 1) => (ms/multiset 1)
        (send-receive 2) => (ms/multiset 2)
        (send-receive 3) => (ms/multiset 3)
        (Thread/sleep 2100)
        (send-receive 4) => (ms/multiset 4)))
    (facts "hash after source (timed)"
      (test-node :hash (t/seconds 2) false #(spark/make-diff-add-node (new-id!) [] [%])
        (send-receive :a 1) => (ms/multiset [:a 1])
        (send-receive :b 1) => (ms/multiset [:b 1])
        (Thread/sleep 2100)
        (send-receive :c 1) => (ms/multiset [:c 1])))
    (facts "multiset after buffer (size 2)"
      (test-node :multiset false false
        #(let [buffer-node (spark/make-buffer-node (new-id!) [2] [%])]
          (spark/make-diff-add-node (new-id!) [] [buffer-node]))
        (send-receive 1) => (ms/multiset 1)
        (send-receive 2) => (ms/multiset 2)
        (send-receive 3) => (ms/multiset 3)))
    (facts "hash after buffer (size 2)"
      (test-node :hash false false
        #(let [buffer-node (spark/make-buffer-node (new-id!) [2] [%])]
          (spark/make-diff-add-node (new-id!) [] [buffer-node]))
        (send-receive :a 1) => (ms/multiset [:a 1])
        (send-receive :a 2) => (ms/multiset [:a 2])
        (send-receive :a 3) => (ms/multiset [:a 3]))))

  (facts "about diff-remove"
    (facts "multiset after source (timed)"
      (test-node :multiset (t/seconds 2) false #(spark/make-diff-remove-node (new-id!) [] [%])
        (send-receive 1) => (ms/multiset)
        (send-receive 2) => (ms/multiset)
        (send-receive 3) => (ms/multiset)
        (Thread/sleep 2100)
        (send-receive 4) => (ms/multiset 1 2 3)))
    (facts "hash after source (timed)"
      (test-node :hash (t/seconds 2) false #(spark/make-diff-remove-node (new-id!) [] [%])
        (send-receive :a 1) => (ms/multiset)
        (send-receive :b 1) => (ms/multiset)
        (Thread/sleep 2100)
        (send-receive :c 1) => (ms/multiset [:a 1] [:b 1])))
    (facts "multiset after buffer (size 2)"
      (test-node :multiset false false
        #(let [buffer-node (spark/make-buffer-node (new-id!) [2] [%])]
          (spark/make-diff-remove-node (new-id!) [] [buffer-node]))
        (send-receive 1) => (ms/multiset)
        (send-receive 2) => (ms/multiset)
        (send-receive 3) => (ms/multiset 1)))
    (facts "hash after buffer (size 2)"
      (test-node :hash false false
        #(let [buffer-node (spark/make-buffer-node (new-id!) [2] [%])]
          (spark/make-diff-remove-node (new-id!) [] [buffer-node]))
        (send-receive :a 1) => (ms/multiset)
        (send-receive :a 2) => (ms/multiset)
        (send-receive :a 3) => (ms/multiset [:a 1])))))

;; hash
(comment
  (facts "about map-by-key, function inc"
    (test-hash-node #(spark/make-map-by-key-node (new-id!) [spark-inc] [%])
      (send-receive :a 1) => {:a (ms/multiset 2)}
      (send-receive :b 1) => {:a (ms/multiset 2) :b (ms/multiset 2)}
      (send-receive :b 2) => {:a (ms/multiset 2) :b (ms/multiset 2 3)}))

  (facts "about filter-by-key, function even?"
    (test-hash-node #(spark/make-filter-by-key-node (new-id!) [even?] [%])
      (send-receive :a 1) => {}
      (send-receive :b 1) => {}
      (send-receive :a 2) => {:a (ms/multiset 2)}
      (send-receive :b 2) => {:a (ms/multiset 2) :b (ms/multiset 2)}))

  (facts "about filter-key-size node (size 2)"
    (test-hash-node #(spark/make-filter-key-size-node (new-id!) [2] [%])
      (send-receive :a 1) => {}
      (send-receive :b 1) => {}
      (send-receive :a 2) => {:a (ms/multiset 1 2)}
      (send-receive :b 2) => {:a (ms/multiset 1 2) :b (ms/multiset 1 2)}
      (send-receive :b 3) => {:a (ms/multiset 1 2) :b (ms/multiset 1 2 3)}))

  (facts "about reduce-by-key, function (fn [a b] (+ a b))"
    (facts "hash (no initial)"
      (test-hash-node #(spark/make-reduce-by-key-node (new-id!) [reduce-fn false] [%])
        (send-receive :a 1) => {:a (ms/multiset 1)}
        (send-receive :a 2) => {:a (ms/multiset 3)}
        (send-receive :a 3) => {:a (ms/multiset 6)}
        (send-receive :b 1) => {:a (ms/multiset 6) :b (ms/multiset 1)}
        (send-receive :b 2) => {:a (ms/multiset 6) :b (ms/multiset 3)}))
    (comment (facts "hash (initial = -1)"
               (test-hash-node #(spark/make-reduce-by-key-node (new-id!) [reduce-fn {:val -1}] [%])
                 (send-receive :a 1) => {:a (ms/multiset 0)}
                 (send-receive :a 2) => {:a (ms/multiset 2)}
                 (send-receive :a 3) => {:a (ms/multiset 5)}
                 (send-receive :b 1) => {:a (ms/multiset 5) :b (ms/multiset 0)}
                 (send-receive :b 2) => {:a (ms/multiset 5) :b (ms/multiset 2)}))))

  (facts "about hash-to-multiset"
    (test-hash-node #(spark/make-hash-to-multiset-node (new-id!) [] [%])
      (send-receive :a 1) => (ms/multiset [:a 1])
      (send-receive :b 1) => (ms/multiset [:a 1] [:b 1])
      (send-receive :b 2) => (ms/multiset [:a 1] [:b 1] [:b 2]))))

;; multisets
(facts "about map, function inc"
  (test-multiset-node #(spark/make-map-node (new-id!) [spark-inc] [%])
    (send-receive 1) => (ms/multiset 2)
    (send-receive 2) => (ms/multiset 2 3)
    (send-receive 3) => (ms/multiset 2 3 4)))

(facts "about reduce, function (fn [a b] (+ a b))"
  (facts "multiset (no initial)"
    (test-multiset-node #(spark/make-reduce-node (new-id!) [reduce-fn false] [%])
      (send-receive 1) => (ms/multiset 1)
      (send-receive 2) => (ms/multiset 3)
      (send-receive 3) => (ms/multiset 6)
      (send-receive 3) => (ms/multiset 9)
      (send-receive 3) => (ms/multiset 12)
      (send-receive 10) => (ms/multiset 22)
      (send-receive 10) => (ms/multiset 32)))
  (comment (facts "multiset (initial = -1)"
             (test-multiset-node #(spark/make-reduce-node (new-id!) [reduce-fn {:val -1}] [%])
               (send-receive 1) => (ms/multiset 0)
               (send-receive 2) => (ms/multiset 2)
               (send-receive 3) => (ms/multiset 5)))))

(facts "about filter, function even?"
  (test-multiset-node #(spark/make-filter-node (new-id!) [even?] [%])
    (send-receive 1) => (ms/multiset)
    (send-receive 2) => (ms/multiset 2)
    (send-receive 3) => (ms/multiset 2)))

(facts "about make-multiplicities-node"
  (test-multiset-node #(spark/make-multiplicities-node (new-id!) [] [%])
    (send-receive 'a) => (ms/multiset ['a 1])
    (send-receive 'b) => (ms/multiset ['a 1] ['b 1])
    (send-receive 'b) => (ms/multiset ['a 1] ['b 2])))

(facts "about union"
  (test-binode :multiset #(spark/make-union-node (new-id!) [] [%1 %2])
    (bi-send-receive 1 1) => (ms/multiset 1)
    (bi-send-receive 2 2) => (ms/multiset 1 2)
    (bi-send-receive 1 2) => (ms/multiset 1 2)
    (bi-send-receive 1 1) => (ms/multiset 1 1 2)))

(facts "about subtract"
  (test-binode :multiset #(spark/make-subtract-node (new-id!) [] [%1 %2])
    (bi-send-receive 1 1) => (ms/multiset 1)
    (bi-send-receive 2 2) => (ms/multiset 1)
    (bi-send-receive 1 2) => (ms/multiset)
    (bi-send-receive 1 1) => (ms/multiset 1)))

(facts "about intersection"
  (test-binode :multiset #(spark/make-intersection-node (new-id!) [] [%1 %2])
    (bi-send-receive 1 1) => (ms/multiset)
    (bi-send-receive 2 2) => (ms/multiset)
    (bi-send-receive 1 2) => (ms/multiset 1)
    (bi-send-receive 2 1) => (ms/multiset 1 2)
    (bi-send-receive 2 1)
    (bi-send-receive 2 2) => (ms/multiset 1 2 2)))

(comment
  (facts "about distinct"
    (test-multiset-node #(spark/make-distinct-node (new-id!) [] [%])
      (send-receive 2) => (ms/multiset 2)
      (send-receive 1) => (ms/multiset 1 2)
      (send-receive 2) => (ms/multiset 1 2))))

(do-tests)
