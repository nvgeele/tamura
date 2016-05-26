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
            [tamura.values :as v])
  (:use [tamura.datastructures])
  (:import [redis.clients.jedis Jedis]))

(p/import-vars
  [tamura.macros
   def
   defn
   defsig])

(defmacro ^{:private true} assert-args
  [& pairs]
  `(do (when-not ~(first pairs)
         (throw (IllegalArgumentException.
                  (str (first ~'&form) " requires " ~(second pairs) " in " ~'*ns* ":" (:line (meta ~'&form))))))
       ~(let [more (nnext pairs)]
          (when more
            (list* `assert-args more)))))

(defmacro ^{:private true} assert*
  [& pairs]
  (assert-args
    (>= (count pairs) 2) "2 or more expressions in the body"
    (even? (count pairs)) "an even amount of expressions")
  `(do (assert ~(first pairs) ~(second pairs))
       ~(let [more (nnext pairs)]
          (when more
            (list* `assert* more)))))

(defmacro when-let*
  "bindings => [bindingform test ...]"
  [bindings & body]
  (assert-args
    (vector? bindings) "a vector for its binding"
    (>= (count bindings) 2) "2 or more forms in binding vector"
    (even? (count bindings)) "even amount of bindings")
  (let [form (bindings 0) tst (bindings 1)]
    `(let [temp# ~tst]
       (when temp#
         (let [~form temp#
               ~@(rest (rest bindings))]
           ~@body)))))

(core/defn ormap
  [f lst]
  (loop [l lst]
    (cond
      (empty? l) false
      (f (first l)) true
      :else (recur (rest l)))))

(core/defn andmap
  [f lst]
  (loop [l lst]
    (cond (empty? l) true
          (f (first l)) (recur (rest lst))
          :else false)))

;; TODO: define some sinks
;; TODO: all sink operators are Actors, not Reactors;; make sure nothing happens when changed? = false
;; TODO: let coordinator keep list of producers, so we can have something like (coordinator-start) to kick-start errting
(defrecord Coordinator [in])
(defrecord Node [id node-type return-type sub-chan])
(defrecord Source [id node-type return-type sub-chan in]) ;; isa Node
(defrecord Sink [id node-type])                           ;; isa Node

(def buffer-size 32)
(defmacro chan
  ([] `(a/chan ~buffer-size))
  ([size] `(a/chan ~size)))
(def counter (atom 0))
(core/defn new-id!
  []
  (swap! counter inc))

(defmacro node-subscribe
  [source channel]
  `(>!! (:sub-chan ~source) {:subscribe ~channel}))

(declare ^:dynamic ^:private *coordinator*)

(def nodes (atom {}))
(def sources (atom []))
(def threads (atom []))
(def node-constructors (atom {}))

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
(core/defn build-nodes! []
  (loop [sorted (sort-nodes)]
    (if (empty? sorted)
      true
      (let [id (first sorted)
            node (get @nodes id)
            inputs (core/map #(:node (get @nodes %)) (:inputs node))
            node-obj ((get @node-constructors (:node-type node)) id (:args node) inputs)]
        (swap! nodes update-in [id :node] (constantly node-obj))
        (if (or (= (:node-type node) ::source)
                (contains? (ancestors (:node-type node)) ::source))
          (>!! (:in *coordinator*) {:new-source (:in node-obj)})
          (>!! (:in *coordinator*) :else))
        (recur (rest sorted))))))

(core/defn register-constructor!
  [node-type constructor]
  (swap! node-constructors assoc node-type constructor))

(core/defn- started?
  []
  (let [c (chan 0)
        s (do (>!! (:in *coordinator*) {:started? c})
              (<!! c))]
    (a/close! c)
    s))

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

;; TODO: phase2 of multiclock reactive programming (detect when construction is done) (or cheat and Thread/sleep)
(core/defn make-coordinator
  []
  (let [in (chan)]
    (go-loop [msg (<! in)
              started? false
              sources []]
      (log/debug (str "coordinator received: " msg))
      (match msg
        {:new-source source-chan}
        (if started?
          (throw (Exception. "can not add new sources when already running"))
          (recur (<! in) started? (cons source-chan sources)))

        {:destination id :value value}
        (do (when started?
              (doseq [source sources]
                (>! source msg)))
            (recur (<! in) started? sources))

        {:started? reply-channel}
        (do (>! reply-channel started?)
            (recur (<! in) started? sources))

        :start (recur (<! in) true sources)

        :stop (recur (<! in) false [])

        :reset (recur (<! in) false [])

        :else (recur (<! in) started? sources)))
    (Coordinator. in)))

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

;; TODO: use this node when Cursive has decent macro support
(defmacro defnode
  [constructor-name type args & body]
  `(do (core/defn ~constructor-name ~args ~@body)
       (register-constructor! ~type ~constructor-name)))

;; NOTE: timeout must be a period (e.g. t/minutes)
;; TODO: leasing when no data has changed?
;; TODO: ping node to do leasing now and then

;; TODO: use buffered and timed datastructures
;; TODO: send internal hash and multiset from above data structures
(core/defn make-source-node
  [id [return-type & {:keys [timeout buffer] :or {timeout false buffer false}}] []]
  (let [in (chan)
        transformer (if (= return-type :multiset)
                      #(to-regular-multiset %)
                      #(to-regular-hash %))]
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
    (Source. id ::source return-type in in)))
(register-constructor! ::source make-source-node)

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
        (println "Redis input received:" value)
        (>!! (:in *coordinator*) {:destination id :value value})
        (recur)))
    source-node))
(register-constructor! ::redis make-redis-node)
(derive ::redis ::source)

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
    (Sink. id ::do-apply)))
(register-constructor! ::do-apply make-do-apply-node)

;; TODO: put in docstring that it emits empty hash or set
;; TODO: make sure it still works with leasing

(core/defn make-delay-node
  [id [] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        hash-input? (= (:return-type input-node) :hash)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              previous (if hash-input? (make-hash) (make-multiset))]
      (log/debug (str "delay-node " id " has received: " msg))
      (send-subscribers @subscribers (:changed? msg) previous id)
      (recur (<! input) (if (:changed? msg) (:value msg) previous)))
    (Node. id ::delay (:return-type input-node) sub-chan)))
(register-constructor! ::delay make-delay-node)

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
    (Node. id ::multiplicities :multiset sub-chan)))
(register-constructor! ::multiplicities make-multiplicities-node)

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
    (Node. id ::reduce :multiset sub-chan)))
(register-constructor! ::reduce make-reduce-node)

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
    (Node. id ::reduce-by-key :hash sub-chan)))
(register-constructor! ::reduce-by-key make-reduce-by-key-node)

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
    (Node. id ::throttle (:return-type input-node) sub-chan)))
(register-constructor! ::throttle make-throttle-node)

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
    (Node. id ::buffer (:return-type input-node) sub-chan)))
(register-constructor! ::buffer make-buffer-node)

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
    (Node. id ::diff-add (:return-type input-node) sub-chan)))
(register-constructor! ::diff-add make-diff-add-node)

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
    (Node. id ::diff-remove (:return-type input-node) sub-chan)))
(register-constructor! ::diff-remove make-diff-remove-node)

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
    (Node. id ::filter-key-size :hash sub-chan)))
(register-constructor! ::filter-key-size make-filter-key-size-node)

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
    (Node. id ::hash-to-multiset :multiset sub-chan)))
(register-constructor! ::hash-to-multiset make-hash-to-multiset-node)

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
    (Node. id ::map :multiset sub-chan)))
(register-constructor! ::map make-map-node)

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
    (Node. id ::map-by-key :hash sub-chan)))
(register-constructor! ::map-by-key make-map-by-key-node)

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
    (Node. id ::filter :multiset sub-chan)))
(register-constructor! ::filter make-filter-node)

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
    (Node. id ::filter-by-key :hash sub-chan)))
(register-constructor! ::filter-by-key make-filter-by-key-node)

(def ^:dynamic ^:private *coordinator* (make-coordinator))

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
    (make-signal (register-source! ::redis return-type [return-type host queue key buffer timeout]))))

;; TODO: make this a sink
(core/defn do-apply
  [f arg & args]
  (assert*
    (andmap v/signal? (cons arg args)) "only signals from the second argument on")
  (let [inputs (core/map v/value (cons arg args))
        node (register-node! ::do-apply nil [f] inputs)]
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
        node (register-node! ::delay return-type [] [input])]
    (make-signal node)))

(core/defn multiplicities
  [arg]
  (assert*
    (v/signal? arg) "argument to multiplicities should be a signal"
    (= (:return-type (get-node (v/value arg))) :multiset) "input for multiplicities must be multiset")
  (make-signal (register-node! ::multiplicities :multiset [] [(v/value arg)])))

;; NOTE: Because multisets have no order, the function must be both commutative and associative
(core/defn reduce
  ([source f]
   (assert*
     (v/signal? source) "argument to reduce should be a signal"
     (= (:return-type (get-node (v/value source))) :multiset) "input for reduce must be a multiset")
   (make-signal (register-node! ::reduce :multiset [f false] [(v/value source)])))
  ([source f val]
   (assert*
     (v/signal? source) "argument to reduce should be a signal"
     (= (:return-type (get-node (v/value source))) :multiset) "input for reduce must be a multiset")
   (make-signal (register-node! ::reduce :multiset [f {:val val}] [(v/value source)]))))

;; We *must* work with a node that signals the coordinator to ensure correct propagation in situations where
;; nodes depend on a throttle signal and one or more other signals.
;; TODO: something something initialisation?
;; TODO: what if the input hasn't changed next time we trigger? Do we still propagate a "change"?
(core/defn throttle
  [signal ms]
  (assert*
    (v/signal? signal) "first argument of throttle must be signal")
  (let [trigger (register-source! ::source :multiset [:multiset])
        return-type (:return-type (get-node (v/value signal)))
        node (register-node! ::throttle return-type [ms] [(v/value signal) trigger])]
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
    (or (= (:node-type (get-node (v/value sig))) ::source)
        (contains? (ancestors (:node-type (get-node (v/value sig)))) ::source))
    "input for buffer node should be a source")
  (make-signal (register-node! ::buffer (:return-type (get-node (v/value sig))) [size] [(v/value sig)])))

(core/defn diff-add
  [sig]
  (assert*
    (v/signal? sig) "first argument to diff-add should be a signal"
    (or (contains? [::buffer ::source ::delay] (:node-type (get-node (v/value sig))))
        (contains? (ancestors (:node-type (get-node (v/value sig)))) ::source))
    "input for diff-add node should be a source, buffer, or delay")
  (make-signal (register-node! ::diff-add (:return-type (get-node (v/value sig))) [] [(v/value sig)])))

(core/defn diff-remove
  [sig]
  (assert*
    (v/signal? sig) "first argument to diff-remove should be a signal"
    (or (contains? [::buffer ::source ::delay] (:node-type (get-node (v/value sig))))
        (contains? (ancestors (:node-type (get-node (v/value sig)))) ::source))
    "input for diff-remove node should be a source, buffer, or delay")
  (make-signal (register-node! ::diff-remove (:return-type (get-node (v/value sig))) [] [(v/value sig)])))

;; TODO: make sure size > buffer size of buffer or source?
(core/defn filter-key-size
  [source size]
  (assert*
    (v/signal? source) "argument to filter-key-size should be a signal"
    (= (:return-type (get-node (v/value source))) :hash) "input for filter-key-size must be a hash")
  (make-signal (register-node! ::filter-key-size :hash [size] [(v/value source)])))

;; NOTE: Because multisets have no order, the function must be both commutative and associative
(core/defn reduce-by-key
  ([source f]
   (assert*
     (v/signal? source) "argument to reduce-by-key should be a signal"
     (= (:return-type (get-node (v/value source))) :hash) "input for reduce-by-key must be a hash")
   (make-signal (register-node! ::reduce-by-key :hash [f false] [(v/value source)])))
  ([source f val]
   (assert*
     (v/signal? source) "argument to reduce-by-key should be a signal"
     (= (:return-type (get-node (v/value source))) :hash) "input for reduce-by-key must be a hash")
   (make-signal (register-node! ::reduce-by-key :hash [f {:val val}] [(v/value source)]))))

(core/defn hash-to-multiset
  [source]
  (assert*
    (v/signal? source) "argument to hash-to-multiset should be a signal"
    (= (:return-type (get-node (v/value source))) :hash) "input for hash-to-multiset must be a hash")
  (make-signal (register-node! ::hash-to-multiset :multiset [] [(v/value source)])))

;; TODO: like reduce, make it work for regular collections too
(core/defn map
  [source f]
  (assert*
    (v/signal? source) "argument to map should be a signal"
    (= (:return-type (get-node (v/value source))) :multiset) "input for map must be a multiset")
  (make-signal (register-node! ::map :multiset [f] [(v/value source)])))

(core/defn map-by-key
  [source f]
  (assert*
    (v/signal? source) "argument to map-by-key should be a signal"
    (= (:return-type (get-node (v/value source))) :multiset) "input for map-by-key must be a hash")
  (make-signal (register-node! ::map-by-key :hash [f] [(v/value source)])))

;; TODO: like reduce, make it work for regular collections too
(core/defn filter
  [source f]
  (assert*
    (v/signal? source) "argument to filter should be a signal"
    (= (:return-type (get-node (v/value source))) :multiset) "input for filter must be a multiset")
  (make-signal (register-node! ::filter :multiset [f] [(v/value source)])))

(core/defn filter-by-key
  [source f]
  (assert*
    (v/signal? source) "argument to filter-by-key should be a signal"
    (= (:return-type (get-node (v/value source))) :hash) "input for filter-by-key must be a hash")
  (make-signal (register-node! ::filter-by-key :hash [f] [(v/value source)])))

(defmacro print-signal
  [signal]
  `(do-apply #(locking *out* (println (quote ~signal) ": " %)) ~signal))

(core/defn -main
  [& args]
  (println "Ready"))