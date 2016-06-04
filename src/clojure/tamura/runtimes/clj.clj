(ns tamura.runtimes.clj
  (:require [clojure.core.async :refer [>!! >! <!! <! go go-loop]]
            [clojure.core.match :refer [match]]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [clojure.pprint :refer [pprint]]
            [multiset.core :as ms]
            [tamura.node-types :as nt])
  (:use [tamura.coordinator]
        [tamura.datastructures]
        [tamura.node]
        [tamura.util])
  (:import [redis.clients.jedis Jedis]))

;; NOTE: timeout must be a period (e.g. t/minutes)
;; TODO: leasing when no data has changed?
;; TODO: ping node to do leasing now and then

;; TODO: use buffered and timed datastructures
;; TODO: send internal hash and multiset from above data structures
(defn make-source-node
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
    (make-source id nt/source return-type in in)))
(register-constructor! :clj nt/source make-source-node)

(defn make-redis-node
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
(defn make-do-apply-node
  [id [action] input-nodes]
  (let [inputs (subscribe-inputs input-nodes)
        selectors (map #(if (= (:return-type %) :hash) to-hash to-multiset) input-nodes)]
    (go-loop [msgs
              (map <!! inputs)
              #_(<! (first inputs))
              #_(for [input inputs] (<! input))
              ]
      (log/debug (str "do-apply-node " id " has received: " (seq msgs)))
      (when (ormap :changed? msgs)
        (let [colls (map #(%1 (:value %2)) selectors msgs)]
          (apply action colls)))
      (recur (map <!! inputs)
             #_(<! (first inputs))
             #_(for [input inputs] (<! input))))
    (make-sink id nt/do-apply)))
(register-constructor! :clj nt/do-apply make-do-apply-node)

;; TODO: put in docstring that it emits empty hash or set
;; TODO: make sure it still works with leasing

(defn make-delay-node
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
    (make-node id nt/delay (:return-type input-node) sub-chan)))
(register-constructor! :clj nt/delay make-delay-node)

(defn make-multiplicities-node
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
    (make-node id nt/multiplicities :multiset sub-chan)))
(register-constructor! :clj nt/multiplicities make-multiplicities-node)

(defn make-reduce-node
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
    (make-node id nt/reduce :multiset sub-chan)))
(register-constructor! :clj nt/reduce make-reduce-node)

(defn make-reduce-by-key-node
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
    (make-node id nt/reduce-by-key :hash sub-chan)))
(register-constructor! :clj nt/reduce-by-key make-reduce-by-key-node)

;; NOTE: Because of the throttle nodes, sources now also propagate even when they haven't received an initial value.
;; The reason for this is that if we would not do this, the buffer for the trigger channel would fill up,
;; until the source node(s) for the input-channel produce a value and the trigger channel would be emptied.
;; This could lead to situations where a preliminary message with :changed? true is sent out.
;; The rationale is that a node can only produce a :changed? true value iff all its inputs have been true at least once.
;; TODO: make throttle that propages true on every tick AND one that only propagates true if something has changed since
(defn make-throttle-node
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
    (make-node id nt/throttle (:return-type input-node) sub-chan)))
(register-constructor! :clj nt/throttle make-throttle-node)

(defn make-buffer-node
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
    (make-node id nt/buffer (:return-type input-node) sub-chan)))
(register-constructor! :clj nt/buffer make-buffer-node)

(defn make-diff-add-node
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
    (make-node id nt/diff-add :multiset sub-chan)))
(register-constructor! :clj nt/diff-add make-diff-add-node)

(defn make-diff-remove-node
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
    (make-node id nt/diff-remove :multiset sub-chan)))
(register-constructor! :clj nt/diff-remove make-diff-remove-node)

(defn make-filter-key-size-node
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
    (make-node id nt/filter-key-size :hash sub-chan)))
(register-constructor! :clj nt/filter-key-size make-filter-key-size-node)

(defn make-hash-to-multiset-node
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
    (make-node id nt/hash-to-multiset :multiset sub-chan)))
(register-constructor! :clj nt/hash-to-multiset make-hash-to-multiset-node)

(defn make-map-node
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
    (make-node id nt/map :multiset sub-chan)))
(register-constructor! :clj nt/map make-map-node)

(defn make-map-by-key-node
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
    (make-node id nt/map-by-key :hash sub-chan)))
(register-constructor! :clj nt/map-by-key make-map-by-key-node)

(defn make-filter-node
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
    (make-node id nt/filter :multiset sub-chan)))
(register-constructor! :clj nt/filter make-filter-node)

(defn make-filter-by-key-node
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
    (make-node id nt/filter-by-key :hash sub-chan)))
(register-constructor! :clj nt/filter-by-key make-filter-by-key-node)

(def ^:private print-lock (Object.))
(defn make-print-node
  [id [form] [input-node]]
  (let [input (subscribe-input input-node)
        selector (if (= (:return-type input-node) :hash) to-hash to-multiset)]
    (go-loop [msg (<! input)]
      (log/debug (str "print node " id " has received: " msg))
      (when (:changed? msg)
        (let [s (with-out-str (-> (:value msg)
                                  (selector)
                                  (pprint)))]
          (locking print-lock
            (print (str form ": " s))
            (flush))))
      (recur (<! input)))
    (make-sink id nt/print)))
(register-constructor! :clj nt/print make-print-node)