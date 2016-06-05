(ns tamura.core
  (:refer-clojure :exclude [defn reset! delay reduce map filter print distinct])
  (:require [clojure.core :as core]
            [clojure.core.async :as a :refer [>!! >! <!! <! go go-loop]]
            [clojure.core.match :refer [match]]
            [potemkin :as p]
            [tamura.config :as cfg]
            [tamura.macros :as macros]
            [tamura.node-types :as nt]
            ;; We have to refer to runtimes as to make sure the constructors are registered and loaded
            [tamura.runtimes.clj :as crt]
            [tamura.runtimes.spark :as spark])
  (:use [tamura.coordinator]
        [tamura.datastructures]
        [tamura.node]
        [tamura.util]
        [tamura.values]))

(p/import-vars
  [tamura.macros
   def
   defn
   defsig])

;; TODO: set-up for Spark and so on
(core/defn setup!
  [config]
  (throw (Exception. "ToDo")))

(core/defn start!
  []
  (when (started?)
    (throw (Exception. "already started")))
  (when (= (cfg/runtime) :spark)
    (spark/setup-spark!))
  (build-nodes! (cfg/runtime))
  (if-let [t (cfg/throttle?)]
    (threadloop []
      (Thread/sleep t)
      (>!! (:in *coordinator*) :heartbeat)
      (recur)))
  (swap! threads #(doall (for [t %]
                           (let [thread (Thread. (:body t))]
                             (.start thread)
                             (assoc t :thread thread)))))
  ;; TODO: remove sleep?
  ;; NOTE: we wait one second to ensure all nodes had time to subscribe
  (Thread/sleep 1000)
  (>!! (:in *coordinator*) :start))

;; TODO: way to kill goroutines (or can we rely on garbage collection?)
(core/defn stop!
  []
  (when-not (started?)
    (throw (Exception. "can not stop when not started")))
  (swap! threads #(doall (for [t %]
                           (do (.stop (:thread t))
                               (dissoc t :thread)))))
  (>!! (:in *coordinator*) :stop))

(core/defn reset!
  []
  (when (started?)
    (stop!))
  (core/reset! nodes {})
  (core/reset! sources [])
  (core/reset! counter 0)
  (core/reset! threads [])
  (>!! (:in *coordinator*) :reset))

;;;;           SOURCES           ;;;;

;; TODO: redis sink
;; TODO: put coordinator in Node/Source record? *coordinator* is dynamic...
;; TODO: something something polling time
;; TODO: error if key not present
;; TODO: maybe rename to redis-input
(core/defn redis
  [host queue & {:keys [key buffer timeout] :or {key false buffer false timeout false}}]
  (let [return-type (if key :hash :multiset)]
    (make-signal (register-source! nt/redis return-type [return-type host queue key buffer timeout]))))

;;;;  "POLYMORPHIC" OPERATIONS   ;;;;

;; TODO: constant set
;; TODO: combine-latest
;; TODO: sample-on

(core/defn delay
  [arg]
  (assert*
    (signal? arg) "argument to delay should be a signal")
  (let [input (signal-value arg)
        return-type (:return-type (get-node input))
        node (register-node! nt/delay return-type [] [input])]
    (make-signal node)))

;; TODO: corner case size 0
;; TODO: buffer after delay?
(core/defn buffer
  [sig size]
  (assert*
    (signal? sig) "first argument to buffer should be a signal"
    (nt/source? (:node-type (get-node (signal-value sig)))) "input for buffer node should be a source")
  (make-signal (register-node! nt/buffer (:return-type (get-node (signal-value sig))) [size] [(signal-value sig)])))

(core/defn diff-add
  [sig]
  (let [input-node-type (:node-type (get-node (signal-value sig)))]
    (assert*
      (signal? sig) "first argument to diff-add should be a signal"
      (or (nt/source? input-node-type)
          (nt/buffer? input-node-type)
          (nt/delay? input-node-type))
      "input for diff-add node should be a source, buffer, or delay")
    (make-signal (register-node! nt/diff-add (:return-type (get-node (signal-value sig))) [] [(signal-value sig)]))))

(core/defn diff-remove
  [sig]
  (let [input-node-type (:node-type (get-node (signal-value sig)))]
    (assert*
      (signal? sig) "first argument to diff-remove should be a signal"
      (or (nt/source? input-node-type)
          (nt/buffer? input-node-type)
          (nt/delay? input-node-type))
      "input for diff-remove node should be a source, buffer, or delay")
    (make-signal (register-node! nt/diff-remove (:return-type (get-node (signal-value sig))) [] [(signal-value sig)]))))

;; TODO: make this a sink
(core/defn do-apply
  [f arg & args]
  (assert*
    (andmap signal? (cons arg args)) "only signals from the second argument on")
  (let [inputs (core/map signal-value (cons arg args))
        node (register-node! nt/do-apply nil [f] inputs)]
    (make-signal node)))

;; We *must* work with a node that signals the coordinator to ensure correct propagation in situations where
;; nodes depend on a throttle signal and one or more other signals.
;; TODO: something something initialisation?
;; TODO: what if the input hasn't changed next time we trigger? Do we still propagate a "change"?
;; TODO: create trigger node when constructing?
(core/defn throttle
  [signal ms]
  (assert*
    (signal? signal) "first argument of throttle must be signal")
  (let [trigger (register-source! nt/source :multiset [:multiset])
        return-type (:return-type (get-node (signal-value signal)))
        node (register-node! nt/throttle return-type [ms] [(signal-value signal) trigger])]
    (threadloop []
      (Thread/sleep ms)
      (>!! (:in *coordinator*) {:destination trigger :value nil})
      (recur))
    (make-signal node)))

(defn- print*
  [signal form]
  (assert*
    (signal? signal) "argument to print must be a signal")
  (make-signal (register-sink! nt/print [form] [(signal-value signal)])))

(defmacro print
  [input-form]
  ;; trick to capture the private make-signal and avoid violations
  (let [k print*]
    `(~k ~input-form (quote ~input-form))))

;;;;     MULTISET OPERATIONS     ;;;;

;; TODO: like reduce, make it work for regular collections too
(core/defn map
  [source f]
  (assert*
    (signal? source) "argument to map should be a signal"
    (= (:return-type (get-node (signal-value source))) :multiset) "input for map must be a multiset")
  (make-signal (register-node! nt/map :multiset [f] [(signal-value source)])))

;; NOTE: Because multisets have no order, the function must be both commutative and associative
;; TODO: reduce code duplication
(core/defn reduce
  ([source f]
   (assert*
     (signal? source) "argument to reduce should be a signal"
     (= (:return-type (get-node (signal-value source))) :multiset) "input for reduce must be a multiset")
   (make-signal (register-node! nt/reduce :multiset [f false] [(signal-value source)])))
  ([source f val]
   (assert*
     (signal? source) "argument to reduce should be a signal"
     (= (:return-type (get-node (signal-value source))) :multiset) "input for reduce must be a multiset")
   (make-signal (register-node! nt/reduce :multiset [f {:val val}] [(signal-value source)]))))

;; TODO: like reduce, make it work for regular collections too
(core/defn filter
  [source f]
  (assert*
    (signal? source) "argument to filter should be a signal"
    (= (:return-type (get-node (signal-value source))) :multiset) "input for filter must be a multiset")
  (make-signal (register-node! nt/filter :multiset [f] [(signal-value source)])))

(core/defn multiplicities
  [arg]
  (assert*
    (signal? arg) "argument to multiplicities should be a signal"
    (= (:return-type (get-node (signal-value arg))) :multiset) "input for multiplicities must be multiset")
  (make-signal (register-node! nt/multiplicities :multiset [] [(signal-value arg)])))

(core/defn union
  [left right]
  (assert*
    (and (signal? left) (signal? right)) "arguments to union must be signals"
    (and (= (:return-type (get-node (signal-value left))) :multiset)
         (= (:return-type (get-node (signal-value right))) :multiset))
    "inputs for union must be multisets")
  (make-signal (register-node! nt/union :multiset [] [(signal-value left) (signal-value right)])))

(core/defn subtract
  [left right]
  (assert*
    (and (signal? left) (signal? right)) "arguments to subtract must be signals"
    (and (= (:return-type (get-node (signal-value left))) :multiset)
         (= (:return-type (get-node (signal-value right))) :multiset))
    "inputs for subtract must be multisets")
  (make-signal (register-node! nt/subtract :multiset [] [(signal-value left) (signal-value right)])))

(core/defn intersection
  [left right]
  (assert*
    (and (signal? left) (signal? right)) "arguments to intersection must be signals"
    (and (= (:return-type (get-node (signal-value left))) :multiset)
         (= (:return-type (get-node (signal-value right))) :multiset))
    "inputs for intersection must be multisets")
  (make-signal (register-node! nt/intersection :multiset [] [(signal-value left) (signal-value right)])))

(core/defn distinct
  [input]
  (assert*
    (signal? input) "argument to distinct must be signal"
    (= (:return-type (get-node (signal-value input))) :multiset) "argument to distinct must be multiset")
  (make-signal (register-node! nt/distinct :multiset [] [(signal-value input)])))

;;;;       HASH OPERATIONS       ;;;;

(core/defn map-by-key
  [source f]
  (assert*
    (signal? source) "argument to map-by-key should be a signal"
    (= (:return-type (get-node (signal-value source))) :multiset) "input for map-by-key must be a hash")
  (make-signal (register-node! nt/map-by-key :hash [f] [(signal-value source)])))

;; NOTE: Because multisets have no order, the function must be both commutative and associative
;; TODO: reduce code duplication
(core/defn reduce-by-key
  ([source f]
   (assert*
     (signal? source) "argument to reduce-by-key should be a signal"
     (= (:return-type (get-node (signal-value source))) :hash) "input for reduce-by-key must be a hash")
   (make-signal (register-node! nt/reduce-by-key :hash [f false] [(signal-value source)])))
  ([source f val]
   (assert*
     (signal? source) "argument to reduce-by-key should be a signal"
     (= (:return-type (get-node (signal-value source))) :hash) "input for reduce-by-key must be a hash")
   (make-signal (register-node! nt/reduce-by-key :hash [f {:val val}] [(signal-value source)]))))

(core/defn filter-by-key
  [source f]
  (assert*
    (signal? source) "argument to filter-by-key should be a signal"
    (= (:return-type (get-node (signal-value source))) :hash) "input for filter-by-key must be a hash")
  (make-signal (register-node! nt/filter-by-key :hash [f] [(signal-value source)])))

;; TODO: make sure size > buffer size of buffer or source?
(core/defn filter-key-size
  [source size]
  (assert*
    (signal? source) "argument to filter-key-size should be a signal"
    (= (:return-type (get-node (signal-value source))) :hash) "input for filter-key-size must be a hash")
  (make-signal (register-node! nt/filter-key-size :hash [size] [(signal-value source)])))

(core/defn hash-to-multiset
  [source]
  (assert*
    (signal? source) "argument to hash-to-multiset should be a signal"
    (= (:return-type (get-node (signal-value source))) :hash) "input for hash-to-multiset must be a hash")
  (make-signal (register-node! nt/hash-to-multiset :multiset [] [(signal-value source)])))

(core/defn -main
  [& args]
  (println "Ready"))