(ns tamura.core
  (:require [clojure.core :as core]
            [clojure.core.async :as a :refer [>!! >! <!! <! go go-loop]]
            [clojure.core.match :refer [match]]
            [clojure.edn :as edn]
            [clojure.string :refer [upper-case]]
            [clojure.tools.logging :as log]
            [multiset.core :as ms]
            [potemkin :as p]
            [tamura.macros :as macros]
            [tamura.node-types :as nt]
            [tamura.values :as v])
  (:use [tamura.coordinator]
        [tamura.datastructures]
        [tamura.util])
  (:import [redis.clients.jedis Jedis]))

(p/import-vars
  [tamura.macros
   def
   defn
   defsig])

;; TODO: define some sinks
;; TODO: all sink operators are Actors, not Reactors;; make sure nothing happens when changed? = false
;; TODO: let coordinator keep list of producers, so we can have something like (coordinator-start) to kick-start errting
(defrecord Node [id node-type return-type sub-chan])
(defrecord Source [id node-type return-type sub-chan in]) ;; isa Node
(defrecord Sink [id node-type])                           ;; isa Node

(def counter (atom 0))
(core/defn new-id!
  []
  (swap! counter inc))

(defmacro node-subscribe
  [source channel]
  `(>!! (:sub-chan ~source) {:subscribe ~channel}))

(def nodes (atom {}))
(def sources (atom []))
(def threads (atom []))
(def node-constructors (atom {}))

;; TODO: check that inputs aren't sinks?
(core/defn register-node!
  [node-type return-type args inputs]
  (let [id (new-id!)
        node {:node-type node-type
              :return-type return-type
              :args args
              :inputs inputs}]
    (swap! nodes assoc id node)
    (doseq [input-id inputs]
      (swap! nodes update-in [input-id :outputs] conj id))
    id))

(core/defn register-source!
  [node-type return-type args]
  (let [id (new-id!)
        node {:node-type node-type
              :return-type return-type
              :args args}]
    (swap! sources conj id)
    (swap! nodes assoc id node)
    id))

(core/defn register-sink!
  [node-type args inputs]
  (let [id (new-id!)
        node {:node-type node-type
              :args args
              :inputs inputs
              :sink? true}]
    (swap! nodes assoc id node)
    (doseq [input-id inputs]
      (swap! nodes update-in [input-id :outputs] conj id))
    id))

(core/defn get-node
  [id]
  (get @nodes id))

;; TODO: write appropriate test-code
;; TODO: build graph whilst sorting?
(core/defn sort-nodes
  []
  (let [visited (atom (set []))
        sorted (atom [])]
    (letfn [(sort-rec [node]
              (when-not (contains? @visited node)
                (doseq [output (:outputs (get @nodes node))]
                  (sort-rec output))
                (swap! visited conj node)
                (swap! sorted #(cons node %))))]
      (loop [roots @sources]
        (if (empty? roots)
          @sorted
          (do (sort-rec (first roots))
              (recur (rest roots))))))))

;; TODO: maybe write some tests?
;; TODO: check here that inputs aren't sinks?
(core/defn build-nodes! []
  (loop [sorted (sort-nodes)]
    (if (empty? sorted)
      true
      (let [id (first sorted)
            node (get @nodes id)
            inputs (core/map #(:node (get @nodes %)) (:inputs node))
            node-obj ((get @node-constructors (:node-type node)) id (:args node) inputs)]
        (swap! nodes update-in [id :node] (constantly node-obj))
        (if (nt/source? (:node-type node))
          (>!! (:in *coordinator*) {:new-source (:in node-obj)})
          (>!! (:in *coordinator*) :else))
        (recur (rest sorted))))))

(core/defn register-constructor!
  [runtime node-type constructor]
  (swap! node-constructors assoc-in [runtime node-type] constructor))

(core/defn start!
  []
  (when (started?)
    (throw (Exception. "already started")))
  (build-nodes!)
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

(defmacro thread
  [& body]
  `(let [f# (fn [] ~@body)]
     (swap! threads conj {:body f#})))

(defmacro threadloop
  [bindings & body]
  `(thread (loop ~bindings ~@body)))

(core/defn subscribe-input
  [input]
  (let [c (chan)]
    (node-subscribe input c)
    c))

(core/defn subscribe-inputs
  [inputs]
  (core/map subscribe-input inputs))

;; TODO: use this in all nodes
(defmacro send-subscribers
  [subscribers changed? value id]
  `(doseq [sub# ~subscribers]
     (>! sub# {:changed? ~changed? :value ~value :from ~id})))

(defmacro subscriber-loop
  [id channel subscribers]
  `(go-loop [in# (<! ~channel)]
     (match in#
       {:subscribe c#}
       (do (log/debug (str "node " ~id " has received subscriber message"))
           (if (started?)
             (throw (Exception. "can not add subscribers to nodes when started"))
             (swap! ~subscribers #(cons c# %))))

       :else nil)
     (recur (<! ~channel))))

;; NOTE: timeout must be a period (e.g. t/minutes)
;; TODO: leasing when no data has changed?
;; TODO: ping node to do leasing now and then

;; TODO: use buffered and timed datastructures
;; TODO: send internal hash and multiset from above data structures
(core/defn make-source-node
  [id [return-type & {:keys [timeout buffer] :or {timeout false buffer false}}] []]
  (let [in (chan)
        transformer (if (= return-type :multiset) to-regular-multiset to-regular-hash)]
    (go-loop [msg (<! in)
              subs []
              value (cond (and buffer timeout)
                          (if (= return-type :multiset)
                            (make-timed-buffered-multiset timeout buffer)
                            (make-timed-buffered-hash timeout buffer))
                          buffer
                          (if (= return-type :multiset)
                            (make-buffered-multiset buffer)
                            (make-buffered-hash buffer))
                          timeout
                          (if (= return-type :multiset)
                            (make-timed-multiset timeout)
                            (make-timed-hash timeout))
                          :else
                          (if (= return-type :multiset) (make-multiset) (make-hash)))]
      (log/debug (str "source " id " has received: " (seq msg)))
      (match msg
        {:subscribe subscriber}
        (recur (<! in) (cons subscriber subs) value)

        {:destination id :value new-value}
        (let [new-coll (if (= return-type :multiset)
                         (multiset-insert value new-value)
                         (hash-insert value (first new-value) (second new-value)))]
          (send-subscribers subs true (transformer new-coll) id)
          (recur (<! in) subs new-coll))

        ;; TODO: memoize transformer
        {:destination _}
        (do (send-subscribers subs false (transformer value) id)
            (recur (<! in) subs value))

        ;; TODO: error?
        :else (recur (<! in) subs value)))
    (Source. id nt/source return-type in in)))
(register-constructor! :clj nt/source make-source-node)

(core/defn make-redis-node
  [id [return-type host queue key buffer timeout] []]
  (let [source-node (make-source-node id [return-type :timeout timeout :buffer buffer] [])
        conn (Jedis. host)]
    (threadloop []
      (let [v (second (.blpop conn 0 (into-array String [queue])))
            parsed (edn/read-string v)
            value (if key
                    [(get parsed key) (dissoc parsed key)]
                    parsed)]
        (>!! (:in *coordinator*) {:destination id :value value})
        (recur)))
    source-node))
(register-constructor! :clj nt/redis make-redis-node)

;; input nodes = the actual node records
;; inputs = input channels
;; subscribers = atom with list of subscriber channels

;; TODO: instead of checking if one or more inputs have changed, also check that the value for each input is sane (i.e. a multiset)
;; The rationale is that a node can only produce a :changed? true value iff all its inputs have been true at least once.
;; Possible solution: an inputs-seen flag of some sorts?

;; TODO: tests
(core/defn make-do-apply-node
  [id [action] input-nodes]
  (let [inputs (subscribe-inputs input-nodes)
        selectors (core/map #(if (= (:return-type %) :hash) to-hash to-multiset) input-nodes)]
    (go-loop [msgs
              (core/map <!! inputs)
              #_(<! (first inputs))
              #_(for [input inputs] (<! input))
              ]
      (log/debug (str "do-apply-node " id " has received: " (seq msgs)))
      (when (ormap :changed? msgs)
        (let [colls (core/map #(%1 (:value %2)) selectors msgs)]
          (apply action colls)))
      (recur (core/map <!! inputs)
             #_(<! (first inputs))
             #_(for [input inputs] (<! input))))
    (Sink. id nt/do-apply)))
(register-constructor! :clj nt/do-apply make-do-apply-node)

;; TODO: put in docstring that it emits empty hash or set
;; TODO: make sure it still works with leasing

(core/defn make-delay-node
  [id [] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              previous (if (= (:return-type input-node) :hash) (make-hash) (make-multiset))]
      (log/debug (str "delay-node " id " has received: " msg))
      (send-subscribers @subscribers (:changed? msg) previous id)
      (recur (<! input) (if (:changed? msg) (:value msg) previous)))
    (Node. id nt/delay (:return-type input-node) sub-chan)))
(register-constructor! :clj nt/delay make-delay-node)

(core/defn make-multiplicities-node
  [id [] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (make-multiset)]
      (log/debug (str "multiplicities-node " id " has received: " msg))
      (if (:changed? msg)
        (let [multiplicities (multiset-multiplicities (:value msg))]
          (send-subscribers @subscribers true multiplicities id)
          (recur (<! input) multiplicities))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id nt/multiplicities :multiset sub-chan)))
(register-constructor! :clj nt/multiplicities make-multiplicities-node)

(core/defn make-reduce-node
  [id [f initial] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (make-multiset)]
      (log/debug (str "reduce-node " id " has received: " msg))
      (if (:changed? msg)
        (let [value (if initial
                      (multiset-reduce (:value msg) f (:val initial))
                      (multiset-reduce (:value msg) f))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id nt/reduce :multiset sub-chan)))
(register-constructor! :clj nt/reduce make-reduce-node)

(core/defn make-reduce-by-key-node
  [id [f initial] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (make-hash)]
      (log/debug (str "reduce-by-key-node " id " has received: " msg))
      (if (:changed? msg)
        (let [reduced (if initial
                        (hash-reduce-by-key (:value msg) f (:val initial))
                        (hash-reduce-by-key (:value msg) f))]
          (send-subscribers @subscribers true reduced id)
          (recur (<! input) reduced))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id nt/reduce-by-key :hash sub-chan)))
(register-constructor! :clj nt/reduce-by-key make-reduce-by-key-node)

;; NOTE: Because of the throttle nodes, sources now also propagate even when they haven't received an initial value.
;; The reason for this is that if we would not do this, the buffer for the trigger channel would fill up,
;; until the source node(s) for the input-channel produce a value and the trigger channel would be emptied.
;; This could lead to situations where a preliminary message with :changed? true is sent out.
;; The rationale is that a node can only produce a :changed? true value iff all its inputs have been true at least once.
;; TODO: make throttle that propages true on every tick AND one that only propagates true if something has changed since
(core/defn make-throttle-node
  [id [] [input-node trigger-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        trigger (subscribe-input trigger-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              trig (<! trigger)
              seen-value false]
      (log/debug (str "throttle-node " id " has received: " msg))
      (if (:changed? trig)
        (do (doseq [sub @subscribers]
              (>! sub {:changed? (or seen-value (:changed? msg)) :value (:value msg) :from id}))
            (recur (<! input) (<! trigger) (or seen-value (:changed? msg))))
        (do (doseq [sub @subscribers]
              (>! sub {:changed? false :value (:value msg) :from id}))
            (recur (<! input) (<! trigger) (or seen-value (:changed? msg))))))
    (Node. id nt/throttle (:return-type input-node) sub-chan)))
(register-constructor! :clj nt/throttle make-throttle-node)

(core/defn make-buffer-node
  [id [size] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        hash? (= (:return-type input-node) :hash)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              previous nil
              buffer (if hash? (make-buffered-hash size) (make-buffered-multiset size))]
      (log/debug (str "buffer-node " id " has received: " msg))
      (cond (and (:changed? msg) hash?)
            (let [removed (hash-removed (:value msg))
                  inserted (hash-inserted (:value msg))
                  buffer (hash-insert-and-remove buffer inserted removed)]
              (send-subscribers @subscribers true (to-regular-hash buffer) id)
              (recur (<! input) (:value msg) buffer))

            (:changed? msg)
            (let [new (multiset-inserted (:value msg))
                  removed (multiset-removed (:value msg))
                  buffer (multiset-insert-and-remove buffer new removed)]
              (send-subscribers @subscribers true (to-regular-multiset buffer) id)
              (recur (<! input) (:value msg) buffer))

            :else
            (do (send-subscribers @subscribers false buffer id)
                (recur (<! input) previous buffer))))
    (Node. id nt/buffer (:return-type input-node) sub-chan)))
(register-constructor! :clj nt/buffer make-buffer-node)

(core/defn make-diff-add-node
  [id [] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        hash? (= (:return-type input-node) :hash)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (make-multiset)]
      (log/debug (str "diff-add node" id " has received: " msg))
      (if (:changed? msg)
        (let [inserted ((if hash? hash-inserted multiset-inserted) (:value msg))
              value (make-multiset (apply ms/multiset inserted))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id nt/diff-add :multiset sub-chan)))
(register-constructor! :clj nt/diff-add make-diff-add-node)

(core/defn make-diff-remove-node
  [id [] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        hash? (= (:return-type input-node) :hash)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (make-multiset)]
      (log/debug (str "diff-remove node" id " has received: " msg))
      (if (:changed? msg)
        (let [removed ((if hash? hash-removed multiset-removed) (:value msg))
              value (make-multiset (apply ms/multiset removed))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id nt/diff-remove :multiset sub-chan)))
(register-constructor! :clj nt/diff-remove make-diff-remove-node)

(core/defn make-filter-key-size-node
  [id [size] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (make-hash)]
      (log/debug (str "filter-key-size node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (hash-filter-key-size (:value msg) size)]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id nt/filter-key-size :hash sub-chan)))
(register-constructor! :clj nt/filter-key-size make-filter-key-size-node)

(core/defn make-hash-to-multiset-node
  [id [] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (make-hash)]
      (log/debug (str "hash-to-multiset node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (hash->multiset (:value msg))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id nt/hash-to-multiset :multiset sub-chan)))
(register-constructor! :clj nt/hash-to-multiset make-hash-to-multiset-node)

(core/defn make-map-node
  [id [f] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (make-multiset)]
      (log/debug (str "map node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (multiset-map (:value msg) f)]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id nt/map :multiset sub-chan)))
(register-constructor! :clj nt/map make-map-node)

(core/defn make-map-by-key-node
  [id [f] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (make-hash)]
      (log/debug (str "map-by-key node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (hash-map-by-key (:value msg) f)]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id nt/map-by-key :hash sub-chan)))
(register-constructor! :clj nt/map-by-key make-map-by-key-node)

(core/defn make-filter-node
  [id [f] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (make-multiset)]
      (log/debug (str "filter node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (multiset-filter (:value msg) f)]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id nt/filter :multiset sub-chan)))
(register-constructor! :clj nt/filter make-filter-node)

(core/defn make-filter-by-key-node
  [id [f] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (make-hash)]
      (log/debug (str "filter-by-key node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (hash-filter-by-key (:value msg) f)]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id nt/filter-by-key :hash sub-chan)))
(register-constructor! :clj nt/filter-by-key make-filter-by-key-node)

(core/defn- make-signal
  [node]
  (v/make-signal node))

;; TODO: redis sink
;; TODO: put coordinator in Node/Source record? *coordinator* is dynamic...
;; TODO: something something polling time
;; TODO: error if key not present
;; TODO: maybe rename to redis-input
(core/defn redis
  [host queue & {:keys [key buffer timeout] :or {key false buffer false timeout false}}]
  (let [return-type (if key :hash :multiset)]
    (make-signal (register-source! nt/redis return-type [return-type host queue key buffer timeout]))))

;; TODO: make this a sink
(core/defn do-apply
  [f arg & args]
  (assert*
    (andmap v/signal? (cons arg args)) "only signals from the second argument on")
  (let [inputs (core/map v/value (cons arg args))
        node (register-node! nt/do-apply nil [f] inputs)]
    (make-signal node)))

;; TODO: constant set
;; TODO: combine-latest
;; TODO: sample-on

(core/defn delay
  [arg]
  (assert*
    (v/signal? arg) "argument to delay should be a signal")
  (let [input (v/value arg)
        return-type (:return-type (get-node input))
        node (register-node! nt/delay return-type [] [input])]
    (make-signal node)))

(core/defn multiplicities
  [arg]
  (assert*
    (v/signal? arg) "argument to multiplicities should be a signal"
    (= (:return-type (get-node (v/value arg))) :multiset) "input for multiplicities must be multiset")
  (make-signal (register-node! nt/multiplicities :multiset [] [(v/value arg)])))

;; NOTE: Because multisets have no order, the function must be both commutative and associative
;; TODO: reduce code duplication
(core/defn reduce
  ([source f]
   (assert*
     (v/signal? source) "argument to reduce should be a signal"
     (= (:return-type (get-node (v/value source))) :multiset) "input for reduce must be a multiset")
   (make-signal (register-node! nt/reduce :multiset [f false] [(v/value source)])))
  ([source f val]
   (assert*
     (v/signal? source) "argument to reduce should be a signal"
     (= (:return-type (get-node (v/value source))) :multiset) "input for reduce must be a multiset")
   (make-signal (register-node! nt/reduce :multiset [f {:val val}] [(v/value source)]))))

;; We *must* work with a node that signals the coordinator to ensure correct propagation in situations where
;; nodes depend on a throttle signal and one or more other signals.
;; TODO: something something initialisation?
;; TODO: what if the input hasn't changed next time we trigger? Do we still propagate a "change"?
;; TODO: create trigger node when constructing?
(core/defn throttle
  [signal ms]
  (assert*
    (v/signal? signal) "first argument of throttle must be signal")
  (let [trigger (register-source! nt/source :multiset [:multiset])
        return-type (:return-type (get-node (v/value signal)))
        node (register-node! nt/throttle return-type [ms] [(v/value signal) trigger])]
    (threadloop []
      (Thread/sleep ms)
      (>!! (:in *coordinator*) {:destination trigger :value nil})
      (recur))
    (make-signal node)))

;; TODO: corner case size 0
;; TODO: buffer after delay?
(core/defn buffer
  [sig size]
  (assert*
    (v/signal? sig) "first argument to buffer should be a signal"
    (nt/source? (:node-type (get-node (v/value sig)))) "input for buffer node should be a source")
  (make-signal (register-node! nt/buffer (:return-type (get-node (v/value sig))) [size] [(v/value sig)])))

(core/defn diff-add
  [sig]
  (let [input-node-type (:node-type (get-node (v/value sig)))]
    (assert*
      (v/signal? sig) "first argument to diff-add should be a signal"
      (or (nt/source? input-node-type)
          (nt/buffer? input-node-type)
          (nt/delay? input-node-type))
      "input for diff-add node should be a source, buffer, or delay")
    (make-signal (register-node! nt/diff-add (:return-type (get-node (v/value sig))) [] [(v/value sig)]))))

(core/defn diff-remove
  [sig]
  (let [input-node-type (:node-type (get-node (v/value sig)))]
    (assert*
      (v/signal? sig) "first argument to diff-remove should be a signal"
      (or (nt/source? input-node-type)
          (nt/buffer? input-node-type)
          (nt/delay? input-node-type))
      "input for diff-remove node should be a source, buffer, or delay")
    (make-signal (register-node! nt/diff-remove (:return-type (get-node (v/value sig))) [] [(v/value sig)]))))

;; TODO: make sure size > buffer size of buffer or source?
(core/defn filter-key-size
  [source size]
  (assert*
    (v/signal? source) "argument to filter-key-size should be a signal"
    (= (:return-type (get-node (v/value source))) :hash) "input for filter-key-size must be a hash")
  (make-signal (register-node! nt/filter-key-size :hash [size] [(v/value source)])))

;; NOTE: Because multisets have no order, the function must be both commutative and associative
;; TODO: reduce code duplication
(core/defn reduce-by-key
  ([source f]
   (assert*
     (v/signal? source) "argument to reduce-by-key should be a signal"
     (= (:return-type (get-node (v/value source))) :hash) "input for reduce-by-key must be a hash")
   (make-signal (register-node! nt/reduce-by-key :hash [f false] [(v/value source)])))
  ([source f val]
   (assert*
     (v/signal? source) "argument to reduce-by-key should be a signal"
     (= (:return-type (get-node (v/value source))) :hash) "input for reduce-by-key must be a hash")
   (make-signal (register-node! nt/reduce-by-key :hash [f {:val val}] [(v/value source)]))))

(core/defn hash-to-multiset
  [source]
  (assert*
    (v/signal? source) "argument to hash-to-multiset should be a signal"
    (= (:return-type (get-node (v/value source))) :hash) "input for hash-to-multiset must be a hash")
  (make-signal (register-node! nt/hash-to-multiset :multiset [] [(v/value source)])))

;; TODO: like reduce, make it work for regular collections too
(core/defn map
  [source f]
  (assert*
    (v/signal? source) "argument to map should be a signal"
    (= (:return-type (get-node (v/value source))) :multiset) "input for map must be a multiset")
  (make-signal (register-node! nt/map :multiset [f] [(v/value source)])))

(core/defn map-by-key
  [source f]
  (assert*
    (v/signal? source) "argument to map-by-key should be a signal"
    (= (:return-type (get-node (v/value source))) :multiset) "input for map-by-key must be a hash")
  (make-signal (register-node! nt/map-by-key :hash [f] [(v/value source)])))

;; TODO: like reduce, make it work for regular collections too
(core/defn filter
  [source f]
  (assert*
    (v/signal? source) "argument to filter should be a signal"
    (= (:return-type (get-node (v/value source))) :multiset) "input for filter must be a multiset")
  (make-signal (register-node! nt/filter :multiset [f] [(v/value source)])))

(core/defn filter-by-key
  [source f]
  (assert*
    (v/signal? source) "argument to filter-by-key should be a signal"
    (= (:return-type (get-node (v/value source))) :hash) "input for filter-by-key must be a hash")
  (make-signal (register-node! nt/filter-by-key :hash [f] [(v/value source)])))

;; TODO: lock on owned object
(core/defn make-print-signal-node
  [id [signal-text] [input]]
  (make-do-apply-node id [#(locking *out* (println signal-text ":" %))] [input]))
(register-constructor! :clj nt/print make-print-signal-node)

(defmacro print-signal
  [signal]
  ;; trick to capture the private make-signal and avoid violations
  (let [k make-signal]
    `(do
       (let [sig# ~signal]
         (assert*
           (v/signal? sig#) "argument to print-signal should be a signal")
         (~k (register-sink! nt/print [(quote ~signal)] [(v/value sig#)]))))))

(comment
  (core/defn print-signal
    [signal]
    (assert*
      (v/signal? signal) "argument to print-signal should be a signal")
    (make-signal (register-sink! nt/print [signal-text] [(v/value signal)])))

  (defmacro print-signal
    [signal]
    `(do-apply #(locking *out* (println (quote ~signal) ": " %)) ~signal)))

(core/defn -main
  [& args]
  (println "Ready"))