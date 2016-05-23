(ns tamura.core-test
  (:use midje.sweet
        tamura.datastructures)
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
;; TODO: buffer!
(defmacro test-node
  [type timeout buffer node-init & body]
  `(let [source-id# (core/new-id!)
         source-node# (core/make-source-node source-id# [~type :timeout ~timeout :buffer ~buffer] [])
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
  [& {:keys [return-type] :or {return-type *current-type*}}]
  (case return-type
    :multiset (receive-multiset)
    :hash (receive-hash)
    (throw (Exception. "*current-type* not bound"))))

(defn send-receive
  ([value]
   (send value)
   (receive))
  ([key val]
   (send-receive [key val])))

(defn send-receive*
  ([value return-type]
   (send value)
   (receive :return-type return-type))
  ([key value return-type]
   (send [key value])
   (receive :return-type return-type)))

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
    (test-multiset-node #(core/make-delay-node (core/new-id!) [] [%])
      (send-receive 1) => (ms/multiset)
      (send-receive 2) => (ms/multiset 1)
      (send-receive 3) => (ms/multiset 1 2)))
  (facts "hash"
    (test-hash-node #(core/make-delay-node (core/new-id!) [] [%])
      (send-receive :a 1) => {}
      (send-receive :b 1) => {:a (ms/multiset 1)}
      (send-receive :a 2) => {:a (ms/multiset 1) :b (ms/multiset 1)}))
  (facts "about delay after leased and buffered node"
    (facts "multiset (buffer size 3)"
      (test-node :multiset (t/seconds 2) 3 #(core/make-delay-node (core/new-id!) [] [%])
        (send-receive 1) => (ms/multiset)
        (send-receive 2) => (ms/multiset 1)
        (send-receive 3) => (ms/multiset 1 2)
        (send-receive 4) => (ms/multiset 1 2 3)
        (send-receive 5) => (ms/multiset 2 3 4)
        (Thread/sleep 2100)
        (send-receive 6) => (ms/multiset 3 4 5)
        (send-receive 7) => (ms/multiset 6)))
    (facts "hash (buffer size 2)"
      (test-node :hash (t/seconds 2) 2 #(core/make-delay-node (core/new-id!) [] [%])
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
    (test-multiset-node #(core/make-buffer-node (core/new-id!) [2] [%])
      (send-receive 1) => (ms/multiset 1)
      (send-receive 2) => (ms/multiset 1 2)
      (send-receive 3) => (ms/multiset 2 3)
      (send-receive 4) => (ms/multiset 3 4)))
  (facts "hash"
    (test-hash-node #(core/make-buffer-node (core/new-id!) [2] [%])
      (send-receive :a 1) => {:a (ms/multiset 1)}
      (send-receive :a 2) => {:a (ms/multiset 1 2)}
      (send-receive :a 3) => {:a (ms/multiset 2 3)}
      (send-receive :a 4) => {:a (ms/multiset 3 4)}))
  (facts "about buffer after leased and buffered node"
    (facts "multiset (source buffer size 3)"
      (test-node :multiset (t/seconds 2) 3 #(core/make-buffer-node (core/new-id!) [2] [%])
        (send-receive 1) => (ms/multiset 1)
        (send-receive 2) => (ms/multiset 1 2)
        (send-receive 3) => (ms/multiset 2 3)
        (Thread/sleep 2100)
        (send-receive 4) => (ms/multiset 4)
        (send-receive 5) => (ms/multiset 4 5)))
    (facts "multiset, buffer of size 3 after buffered source of size 2"
      (test-node :multiset (t/seconds 2) 2 #(core/make-buffer-node (core/new-id!) [3] [%])
        (send-receive 1) => (ms/multiset 1)
        (send-receive 2) => (ms/multiset 1 2)
        (send-receive 3) => (ms/multiset 2 3)
        (send-receive 4) => (ms/multiset 3 4)))
    (facts "hash (source buffer size 3)"
      (test-node :hash (t/seconds 2) 3 #(core/make-buffer-node (core/new-id!) [2] [%])
        (send-receive :a 1) => {:a (ms/multiset 1)}
        (send-receive :a 2) => {:a (ms/multiset 1 2)}
        (send-receive :a 3) => {:a (ms/multiset 2 3)}
        (send-receive :b 1) => {:a (ms/multiset 2 3) :b (ms/multiset 1)}
        (Thread/sleep 2100)
        (send-receive :d 1) => {:d (ms/multiset 1)}
        (send-receive :e 1) => {:d (ms/multiset 1) :e (ms/multiset 1)}))))

(facts "about diff-add"
  (facts "multiset after source (timed)"
    (test-node :multiset (t/seconds 2) false #(core/make-diff-add-node (core/new-id!) [] [%])
      (send-receive 1) => (ms/multiset 1)
      (send-receive 2) => (ms/multiset 2)
      (send-receive 3) => (ms/multiset 3)
      (Thread/sleep 2100)
      (send-receive 4) => (ms/multiset 4)))
  (facts "hash after source (timed)"
    (test-node :hash (t/seconds 2) false #(core/make-diff-add-node (core/new-id!) [] [%])
      (send-receive* :a 1 :multiset) => (ms/multiset [:a 1])
      (send-receive* :b 1 :multiset) => (ms/multiset [:b 1])
      (Thread/sleep 2100)
      (send-receive* :c 1 :multiset) => (ms/multiset [:c 1])))
  (facts "multiset after buffer (size 2)"
    (test-node :multiset false false
      #(let [buffer-node (core/make-buffer-node (core/new-id!) [2] [%])]
        (core/make-diff-add-node (core/new-id!) [] [buffer-node]))
      (send-receive 1) => (ms/multiset 1)
      (send-receive 2) => (ms/multiset 2)
      (send-receive 3) => (ms/multiset 3)))
  (facts "hash after buffer (size 2)"
    (test-node :hash false false
      #(let [buffer-node (core/make-buffer-node (core/new-id!) [2] [%])]
        (core/make-diff-add-node (core/new-id!) [] [buffer-node]))
      (send-receive* :a 1 :multiset) => (ms/multiset [:a 1])
      (send-receive* :a 2 :multiset) => (ms/multiset [:a 2])
      (send-receive* :a 3 :multiset) => (ms/multiset [:a 3]))))

(facts "about diff-remove"
  (facts "multiset after source (timed)"
    (test-node :multiset (t/seconds 2) false #(core/make-diff-remove-node (core/new-id!) [] [%])
      (send-receive 1) => (ms/multiset)
      (send-receive 2) => (ms/multiset)
      (send-receive 3) => (ms/multiset)
      (Thread/sleep 2100)
      (send-receive 4) => (ms/multiset 1 2 3)))
  (facts "hash after source (timed)"
    (test-node :hash (t/seconds 2) false #(core/make-diff-remove-node (core/new-id!) [] [%])
      (send-receive* :a 1 :multiset) => (ms/multiset)
      (send-receive* :b 1 :multiset) => (ms/multiset)
      (Thread/sleep 2100)
      (send-receive* :c 1 :multiset) => (ms/multiset [:a 1] [:b 1])))
  (facts "multiset after buffer (size 2)"
    (test-node :multiset false false
      #(let [buffer-node (core/make-buffer-node (core/new-id!) [2] [%])]
        (core/make-diff-remove-node (core/new-id!) [] [buffer-node]))
      (send-receive 1) => (ms/multiset)
      (send-receive 2) => (ms/multiset)
      (send-receive 3) => (ms/multiset 1)))
  (facts "hash after buffer (size 2)"
    (test-node :hash false false
      #(let [buffer-node (core/make-buffer-node (core/new-id!) [2] [%])]
        (core/make-diff-remove-node (core/new-id!) [] [buffer-node]))
      (send-receive* :a 1 :multiset) => (ms/multiset)
      (send-receive* :a 2 :multiset) => (ms/multiset)
      (send-receive* :a 3 :multiset) => (ms/multiset [:a 1]))))

(facts "about make-multiplicities-node"
  (test-multiset-node #(core/make-multiplicities-node (core/new-id!) [] [%])
    (send-receive 'a) => (ms/multiset ['a 1])
    (send-receive 'b) => (ms/multiset ['a 1] ['b 1])
    (send-receive 'b) => (ms/multiset ['a 1] ['b 2])))

(facts "about filter-key-size node (size 2)"
  (test-hash-node #(core/make-filter-key-size-node (core/new-id!) [2] [%])
    (send-receive :a 1) => {}
    (send-receive :b 1) => {}
    (send-receive :a 2) => {:a (ms/multiset 1 2)}
    (send-receive :b 2) => {:a (ms/multiset 1 2) :b (ms/multiset 1 2)}
    (send-receive :b 3) => {:a (ms/multiset 1 2) :b (ms/multiset 1 2 3)}))

(facts "about reduce-by-key, function (fn [a b] (+ a b))"
  (facts "hash (no initial)"
    (test-hash-node #(core/make-reduce-by-key-node (core/new-id!) [(fn [a b] (+ a b)) false] [%])
      (send-receive :a 1) => {:a (ms/multiset 1)}
      (send-receive :a 2) => {:a (ms/multiset 3)}
      (send-receive :a 3) => {:a (ms/multiset 6)}
      (send-receive :b 1) => {:a (ms/multiset 6) :b (ms/multiset 1)}
      (send-receive :b 2) => {:a (ms/multiset 6) :b (ms/multiset 3)}))
  (facts "hash (initial = -1)"
    (test-hash-node #(core/make-reduce-by-key-node (core/new-id!) [(fn [a b] (+ a b)) {:val -1}] [%])
      (send-receive :a 1) => {:a (ms/multiset 0)}
      (send-receive :a 2) => {:a (ms/multiset 2)}
      (send-receive :a 3) => {:a (ms/multiset 5)}
      (send-receive :b 1) => {:a (ms/multiset 5) :b (ms/multiset 0)}
      (send-receive :b 2) => {:a (ms/multiset 5) :b (ms/multiset 2)})))

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