(ns tamura.core
  (:require [clojure.core :as core]
            [clojure.core.async :as a :refer [>!! <!! go go-loop]]
            [clojure.core.match :refer [match]]
            [clojure.edn :as edn]
            [clojure.set :as cs]
            [clojure.string :refer [upper-case]]
            [clojure.tools.logging :as log]
            [multiset.core :as ms]
            [potemkin :as p]
            [tamura.macros :as macros]
            [tamura.values :as v]
            [tamura.funcs :as funcs])
  (:import [redis.clients.jedis JedisPool]
           [java.util LinkedList]))

(p/import-vars
  [tamura.macros

   def
   defn
   defsig]

  #_[tamura.funcs

   map

   ])

(defmacro install
  "Installs"
  []
  ;; Check if :lang :tamura in metadata?
  ;; Overwrite eval?
  ;; Overwrite macros?

  (println "Installing tamura...")
  nil)

;; intra-actor message: {:changed? false :value nil :origin nil :destination nil}
;; each actor counts how many updates it receives, if updates = parents, then proceed

(defmacro ^{:private true} assert-args
  [& pairs]
  `(do (when-not ~(first pairs)
         (throw (IllegalArgumentException.
                  (str (first ~'&form) " requires " ~(second pairs) " in " ~'*ns* ":" (:line (meta ~'&form))))))
       ~(let [more (nnext pairs)]
          (when more
            (list* `assert-args more)))))

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

(defmacro thread
  [& body]
  `(doto (Thread. (fn [] ~@body))
     (.start)))

(defmacro threadloop
  [bindings & body]
  `(thread (loop ~bindings ~@body)))

;; TODO: develop this further so operations can detect if there is a multiset or not
;; (deftype MultiSet [keyed? mset])

(core/defn make-hash
  ([]
    (make-hash {}))
  ([hash]
    {:type ::hash :hash hash}))

(core/defn hash?
  [x]
  (= (:type x) ::hash))

(core/defn hash-contains?
  [hash key]
  (let [hash (:hash hash)]
    (contains? hash key)))

(core/defn hash-get
  [hash key]
  (let [hash (:hash hash)]
    (get hash key)))

(core/defn hash-keys
  [hash]
  (let [hash (:hash hash)]
    (keys hash)))

(core/defn hash-insert
  [hash key val]
  (make-hash (assoc (:hash hash) key val)))

(core/defn hash->set
  [hash]
  (set (:hash hash)))

(core/defn make-multiset
  ([]
   (make-multiset (ms/multiset)))
  ([multiset]
   {:type ::multiset :multiset multiset}))

(core/defn multiset?
  [x]
  (= (:type x) ::multiset))

(core/defn multiset-contains?
  [ms val]
  (contains? (:multiset ms) val))

(core/defn multiset-multiplicities
  [ms]
  (ms/multiplicities (:multiset ms)))

(core/defn multiset-insert
  [ms val]
  (-> (:multiset ms)
      (conj val)
      (make-multiset)))

;; TODO: remove these one done
(core/defn multiset-get [set val] (throw (Exception. "I'm not supposed to be used")))

;; TODO: define some sinks
;; TODO: all sink operators are Actors, not Reactors;; make sure nothing happens when changed? = false
;; TODO: let coordinator keep list of producers, so we can have something like (coordinator-start) to kick-start errting
(defrecord Coordinator [in])
(defrecord Node [sub-chan id source?])
(defrecord Source [in sub-chan id source?])                 ;; isa Node
(defrecord Sink [id source?])                               ;; isa Node

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

;; TODO: do we still need heights?
;; TODO: phase2 of multiclock reactive programming (detect when construction is done) (or cheat and Thread/sleep)
(core/defn make-coordinator
  []
  (let [in (chan)]
    (go-loop [msg (<!! in)
              sources []]
      (match msg
             {:new-source source-chan}
             (recur (<!! in) (cons source-chan sources))

             {:destination id :value value}
             (do (doseq [source sources]
                   (>!! source msg))
                 (recur (<!! in) sources))

             :else (recur (<!! in) sources)))
    (Coordinator. in)))

;; NOTE: nodes are currently semi-dynamic; subscribers can be added, but inputs not

;; TODO: implement leasing, data accumulation, etc, in the source nodes
;; TODO: priority queue for leasing
(core/defn make-source-node
  []
  (let [in (chan)
        id (new-id!)]
    (go-loop [msg (<!! in)
              subs []
              value nil]
      (log/debug (str "source " id " has received: " (seq msg)))
      (match msg
             {:subscribe subscriber}
             (recur (<!! in) (cons subscriber subs) value)

             {:destination id :value new-value}
             (do (doseq [sub subs]
                   (>!! sub {:changed? true
                             :value new-value
                             :origin id}))
                 (recur (<!! in) subs {:v new-value}))

             {:destination _}
             (do (doseq [sub subs]
                   (>!! sub {:changed? false
                             :value value
                             :origin id}))
                 (recur (<!! in) subs value))

             ;; TODO: error?
             :else (recur (<!! in) subs value)))
    (Source. in in id true)))

(core/defn subscribe-input
  [input]
  (let [c (chan)]
    (node-subscribe input c)
    c))

(core/defn subscribe-inputs
  [inputs]
  (map subscribe-input inputs))

;; TODO: for generic node construction
(defmacro defnode
  [name inputs bindings & body]
  `(throw (Exception. "TODO")))

(defmacro subscriber-loop
  [channel subscribers]
  `(go-loop [in# (<!! ~channel)]
     (match in# {:subscribe c#} (swap! ~subscribers #(cons c# %)) :else nil)
     (recur (<!! ~channel))))

;; input nodes = the actual node records
;; inputs = input channels
;; subscribers = atom with list of subscriber channels

;; TODO: instead of checking if one or more inputs have changed, also check that the value for each input is sane (i.e. a multiset)
;; The rationale is that a node can only produce a :changed? true value iff all its inputs have been true at least once.
;; Possible solution: an inputs-seen flag of some sorts?

;; TODO: construction finish detection
(core/defn make-map-node
  [input-node f]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop sub-chan subscribers)
    (go-loop [msg (<!! input)
              value nil]
      (log/debug (str "map-node " id " has received: " msg))
      (if (:changed? msg)
        (let [values (:mset (:value msg))
              mapped (map f values)
              new-set (make-multiset (:key (:value msg))
                                     (apply ms/multiset mapped))]
          (doseq [sub @subscribers]
            (>!! sub {:changed? true :value new-set :from id}))
          (recur (<!! input) new-set))
        (do (doseq [sub @subscribers]
              (>!! sub {:changed? false :value value :from id}))
            (recur (<!! input) value))))
    (Node. sub-chan id false)))

(core/defn make-map-hash-node
  [input-node f]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop sub-chan subscribers)
    (go-loop [msg (<!! input)
              value (make-hash)]
      (log/debug (str "map-to-hash-node " id " has received: " msg))
      (if (:changed? msg)
        (let [values (if (hash? (:value msg))
                       (:hash (:value msg))
                       (:multiset (:value msg)))
              new-hash (make-hash (reduce (fn [h v]
                                            (let [[k v] (f v)]
                                              (assoc h k v)))
                                          {}
                                          hash))]
          (doseq [sub @subscribers]
            (>!! sub {:changed? true :value new-hash :from id}))
          (recur (<!! input) new-hash))
        (do (doseq [sub @subscribers]
              (>!! sub {:changed? false :value value :from id}))
            (recur (<!! input) value))))
    (Node. sub-chan id false)))

(core/defn make-map-multiset-node
  [input-node f]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop sub-chan subscribers)
    (go-loop [msg (<!! input)
              value (make-multiset)]
      (log/debug (str "map-to-multiset-node " id " has received: " msg))
      (if (:changed? msg)
        (let [values (if (hash? (:value msg))
                       (:hash (:value msg))
                       (:multiset (:value msg)))
              new-set (make-multiset (apply ms/multiset (map f values)))]
          (doseq [sub @subscribers]
            (>!! sub {:changed? true :value new-set :from id}))
          (recur (<!! input) new-set))
        (do (doseq [sub @subscribers]
              (>!! sub {:changed? false :value value :from id}))
            (recur (<!! input) value))))
    (Node. sub-chan id false)))

(core/defn make-do-apply-node
  [input-nodes action]
  (let [id (new-id!)
        inputs (subscribe-inputs input-nodes)]
    (go-loop [msgs (map <!! inputs)]
      (log/debug (str "do-apply-node " id " has received: " (seq msgs)))
      (when (ormap :changed? msgs)
        (let [values (map :value msgs)
              colls (map #(if (hash? %) (:hash %) (:multiset %)) values)]
          (apply action colls)))
      (recur (map <!! inputs)))
    (Sink. id false)))

;; TODO: say false when no new element is added to the filtered set?
(core/defn make-filter-node
  [input-node predicate]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop sub-chan subscribers)
    (go-loop [msg (<!! input)
              value nil]
      (log/debug (str "filter-node " id " has received: " msg))
      (if (:changed? msg)
        (let [values (:mset (:value msg))
              filtered (filter predicate values)
              new-set (make-multiset (:key (:value msg))
                                     (apply ms/multiset filtered))]
          (doseq [sub @subscribers]
            (>!! sub {:changed? true :value new-set :from id}))
          (recur (<!! input) new-set))
        (do (doseq [sub @subscribers]
              (>!! sub {:changed? false :value value :from id}))
            (recur (<!! input) value))))
    (Node. sub-chan id false)))

;; TODO: put in docstring that it emits empty hash or set
(core/defn make-delay-node
  [input-node]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop sub-chan subscribers)
    (go-loop [msg (<!! input)
              previous (set nil)
              delayed nil]
      (log/debug (str "delay-node " id " has received: " msg))
      (cond (and (:changed? msg) (multiset? (:value msg)))
            (let [delayed (or delayed (make-multiset))]
              (doseq [sub @subscribers]
                (>!! sub {:changed? true :value delayed :from id}))
              (recur (<!! input) nil (:value msg)))

            (:changed? msg)
            (let [new-set (hash->set (:value msg))
                  previous-set (hash->set previous)
                  new-element (first (cs/difference new-set previous-set))
                  delayed (if-let [existing (hash-get previous (first new-element))]
                            (hash-insert delayed (first new-element) existing)
                            (or delayed (make-hash)))]
              (doseq [sub @subscribers]
                (>!! sub {:changed? true :value delayed :from id}))
              (recur (<!! input) (:value msg) delayed))

            :else
            (do (doseq [sub @subscribers]
                  (>!! sub {:changed? false :value delayed :from id}))
                (recur (<!! input) previous delayed))))
    (Node. sub-chan id false)))

;; TODO: assert l and r are hashes
(core/defn make-zip-node
  [left-node right-node]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        inputs (subscribe-inputs [left-node right-node])]
    (subscriber-loop sub-chan subscribers)
    (go-loop [[l r] (map <!! inputs)
              zipped (make-multiset)]
      (log/debug (str "zip-node " id " has received: " [l r]))
      (if (or (:changed? l) (:changed? r))
        (let [lh (:value l)
              rh (:value r)
              lkeys (set (hash-keys lh))
              rkeys (set (hash-keys rh))
              in-both (cs/intersection lkeys rkeys)
              hash (reduce (fn [h key]
                             (assoc h
                               key [(hash-get lh key) (hash-get rh key)]))
                           {}
                           in-both)
              zipped (make-hash hash)]
          (doseq [sub @subscribers]
            (>!! sub {:changed? true :value zipped :from id}))
          (recur (map <!! inputs) zipped))
        (do (doseq [sub @subscribers]
              (>!! sub {:changed? false :value zipped :from id}))
            (recur (map <!! inputs) zipped))))
    (Node. sub-chan id false)))

;; TODO: throw error when input set is keyed?
(core/defn make-multiplicities-node
  [input-node]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop sub-chan subscribers)
    (go-loop [msg (<!! input)
              value nil]
      (log/debug (str "multiplicities-node " id " has received: " msg))
      (if (:changed? msg)
        (let [values (:mset (:value msg))
              multiplicities (ms/multiplicities values)
              new-set (make-multiset false (apply ms/multiset (seq multiplicities)))]
          (doseq [sub @subscribers]
            (>!! sub {:changed? true :value new-set :from id}))
          (recur (<!! input) new-set))
        (do (doseq [sub @subscribers]
              (>!! sub {:changed? false :value value :from id}))
            (recur (<!! input) value))))
    (Node. sub-chan id false)))

;; TODO: reduce with initial value
(core/defn make-reduce-node
  [input-node f]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop sub-chan subscribers)
    (go-loop [msg (<!! input)
              value nil]
      (log/debug (str "reduce-node " id " has received: " msg))
      (if (:changed? msg)
        (let [values (:mset (:value msg))
              reduced (if (>= (count values) 2) (reduce f values) nil)
              new-set (make-multiset false (ms/multiset reduced))]
          (doseq [sub @subscribers]
            (>!! sub {:changed? true :value new-set :from id}))
          (recur (<!! input) new-set))
        (do (doseq [sub @subscribers]
              (>!! sub {:changed? false :value value :from id}))
            (recur (<!! input) value))))
    (Node. sub-chan id false)))

;; NOTE: Because of the throttle nodes, sources now also propagate even when they haven't received an initial value.
;; The reason for this is that if we would not do this, the buffer for the trigger channel would fill up,
;; until the source node(s) for the input-channel produce a value and the trigger channel would be emptied.
;; This could lead to situations where a preliminary message with :changed? true is sent out.
;; The rationale is that a node can only produce a :changed? true value iff all its inputs have been true at least once.
(core/defn make-throttle-node
  [input-node trigger-node]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        trigger (subscribe-input trigger-node)]
    (subscriber-loop sub-chan subscribers)
    (go-loop [msg (<!! input)
              trig (<!! trigger)
              seen-value false]
      (log/debug (str "throttle-node " id " has received: " msg))
      (if (:changed? trig)
        (do (doseq [sub @subscribers]
              (>!! sub {:changed? (or seen-value (:changed? msg)) :value (:value msg) :from id}))
            (recur (<!! input) (<!! trigger) (or seen-value (:changed? msg))))
        (do (doseq [sub @subscribers]
              (>!! sub {:changed? false :value (:value msg) :from id}))
            (recur (<!! input) (<!! trigger) (or seen-value (:changed? msg))))))
    (Node. sub-chan id false)))

;; TODO: deal with keyed multisets
(core/defn make-buffer-node
  [input-node size]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop sub-chan subscribers)
    (go-loop [msg (<!! input)
              previous-set nil
              buffer-list (LinkedList.)
              buffer nil]
      (log/debug (str "buffer-node " id " has received: " msg))
      (if (:changed? msg)
        (let [new-element (if previous-set
                            (first (ms/minus (:mset (:value msg)) (:mset previous-set)))
                            (first (:mset (:value msg))))]
          (when (= (count buffer-list) size)
            (.removeLast buffer-list))
          (if new-element
            (do (.addFirst buffer-list new-element)
                (let [buffer (make-multiset (:key (:value msg)) (apply ms/multiset buffer-list))]
                  (doseq [sub @subscribers]
                    (>!! sub {:changed? true :value buffer :from id}))
                  (recur (<!! input) (:value msg) buffer-list buffer)))
            (do (doseq [sub @subscribers]
                  (>!! sub {:changed? true :value buffer :from id}))
                (recur (<!! input) (:value msg) buffer-list buffer))))
        (do (doseq [sub @subscribers]
              (>!! sub {:changed? false :value buffer :from id}))
            (recur (<!! input) previous-set buffer-list buffer))))
    (Node. sub-chan id false)))

(def ^:dynamic *coordinator* (make-coordinator))

;; TODO: redis sink
;; TODO: put coordinator in Node/Source record? *coordinator* is dynamic...
;; TODO: something something polling time
;; TODO: error if key not present
(core/defn make-redis
  [host queue & {:keys [key] :or {key false}}]
  (let [node (make-source-node)
        id (:id node)
        pool (JedisPool. host)
        conn (.getResource pool)]
    (>!! (:in *coordinator*) {:new-source (:in node)})
    (threadloop [values (if key (make-hash) (make-multiset))]
      (let [v (second (.blpop conn 0 (into-array String [queue])))
            parsed (edn/read-string v)
            values (if key
                     (hash-insert values (get parsed key) (dissoc parsed key))
                     (multiset-insert values parsed))]
        (>!! (:in *coordinator*) {:destination id :value values})
        (recur values)))
    (v/make-signal node)))

;; TODO: maybe rename to redis-input
(def redis make-redis)

;; For incremental etc: keep dict of input => output for elements
;; TODO: what if function changes key?
;; Possible solution: prevent by comparing in and output, and make keychanges possible by special map function
(core/defn map
  [f arg]
  (if (v/signal? arg)
    (let [source-node (v/value arg)
          node (make-map-node source-node f)]
      (v/make-signal node))
    (core/map f arg)))

;; TODO: test
(core/defn map-to-hash
  [f arg]
  (if (v/signal? arg)
    (let [source-node (v/value arg)
          node (make-map-hash-node source-node f)]
      (v/make-signal node))
    (throw (Exception. "map-to-hash requires a signal as second argument"))))

;; TODO: test
(core/defn map-to-multiset
  [f arg]
  (if (v/signal? arg)
    (let [source-node (v/value arg)
          node (make-map-multiset-node source-node f)]
      (v/make-signal node))
    (throw (Exception. "map-to-multiset requires a signal as second argument"))))

(core/defn lift
  [f]
  (fn [arg]
    (if (v/signal? arg)
      (map f arg)
      (throw (Exception. "Argument for lifted function should be a signal")))))

;; TODO: make this a sink
(core/defn do-apply
  [f arg & args]
  (if (andmap v/signal? (cons arg args))
    (v/make-signal (make-do-apply-node (map v/value (cons arg args)) f))
    (throw (Exception. "do-apply needs to be applied to signals"))))

(core/defn filter
  [f arg]
  (if (v/signal? arg)
    (let [source-node (v/value arg)
          node (make-filter-node source-node f)]
      (v/make-signal node))
    (core/filter f arg)))

;; TODO: new filter
;; TODO: constant set
;; TODO: combine-latest
;; TODO: sample
;; TODO: foldp

(core/defn delay
  [arg]
  (if (v/signal? arg)
    (let [source-node (v/value arg)
          node (make-delay-node source-node)]
      (v/make-signal node))
    (throw (Exception. "argument to delay should be a signal"))))

(core/defn zip
  [left right]
  (if (and (v/signal? left) (v/signal? right))
    (v/make-signal (make-zip-node (v/value left) (v/value right)))
    (throw (Exception. "arguments to zip should be signals"))))

(core/defn multiplicities
  [arg]
  (if (v/signal? arg)
    (v/make-signal (make-multiplicities-node (v/value arg)))
    (throw (Exception. "argument to delay should be a signal"))))

(core/defn reduce
  ([f arg]
   (if (v/signal? arg)
     (-> (v/value arg)
         (make-reduce-node f)
         (v/make-signal))
     (core/reduce f arg)))
  ([f val coll] (core/reduce f val coll)))

;; We *must* work with a node that signals the coordinator to ensure correct propagation in situations where
;; nodes depend on a throttle signal and one or more other signals.
;; TODO: something something initialisation?
;; TODO: what if the input hasn't changed next time we trigger? Do we still propagate a "change"?
(core/defn throttle
  [signal ms]
  (if (v/signal? signal)
    (let [source-node (make-source-node)
          throttle-node (make-throttle-node (v/value signal) source-node)]
      (>!! (:in *coordinator*) {:new-source (:in source-node)})
      (threadloop []
        (Thread/sleep ms)
        (>!! (:in *coordinator*) {:destination (:id source-node) :value true})
        (recur))
      (v/make-signal throttle-node))
    (throw (Exception. "first argument of throttle must be signal"))))

(core/defn buffer
  [sig size]
  (if (v/signal? sig)
    (let [source-node (v/value sig)
          node (make-buffer-node source-node size)]
      (v/make-signal node))
    (throw (Exception. "first argument to delay should be a signal"))))

;; TODO: previous (or is this delay? or do latch instead so we can chain?)
(core/defn previous
  [& args]
  (throw (Exception. "TODO")))

(defmacro print-signal
  [signal]
  `(do-apply #(println (str (quote ~signal) ": " %)) ~signal))

(core/defn -main
  [& args]
  (let [rh (make-redis "localhost" "tqh" :key :id)
        delayed (delay rh)
        zipped (zip rh delayed)]
    ;(print-signal rh)
    ;(print-signal delayed)
    (print-signal zipped)
    )

  (let [rms (make-redis "localhost" "tqms")
        delayed (delay rms)]
    (print-signal rms)
    (print-signal delayed))

  (println "Ready"))