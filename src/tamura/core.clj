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
  (:import [redis.clients.jedis JedisPool]))

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

;; TODO: accessor functions
;; TODO: assert keyed is false or a keyword
(core/defn make-multiset
  ([] (make-multiset false (ms/multiset)))
  ([keyed?] (make-multiset keyed? (ms/multiset)))
  ([keyed? mset] {:type ::multiset
                  :keyed? keyed?
                  :key keyed?
                  :mset mset}))

(core/defn multiset?
  [x]
  (= ::multiset (:type x)))

(core/defn multiset-keyed?
  [x]
  (and (multiset? x)
       (:keyed? x)))

(core/defn multiset-get
  [set val]
  (if (multiset-keyed? set)
    (let [r (filter #(= (get % (:key set)) val) (:mset set))]
      (if r
        (first r)
        false))
    nil))

(core/defn multiset-insert
  [set value]
  (if (multiset-keyed? set)
    (make-multiset (:key set)
                   (if-let [e (multiset-get set (get value (:key set)))]
                     (conj (disj (:mset set) e) value)
                     (conj (:mset set) value)))
    (make-multiset false (conj (:mset set) value))))

;; TODO: define some sinks
;; TODO: all sink operators are Actors, not Reactors;; make sure nothing happens when changed? = false
(defrecord Coordinator [in])
(defrecord Node [sub-chan id source?])
(defrecord Source [in sub-chan id source?])                 ;; isa Node
(defrecord Sink [in id source?])                            ;; isa Node

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

;; nodes are semi-dynamic; subscribers can be added, but inputs not

;; value is wrapped in a map once we receive a value.
;; This way, during propagation, we know if the node is initialised or not.
;; All source nodes in a DAG *must* receive at least one value before anything meaningful can happen really.
;; TODO: change so id is tested explicitely and test if bugs still occur
(core/defn make-source-node
  []
  (let [in (chan)
        id (new-id!)]
    (go-loop [msg (<!! in)
              subs []
              value false]
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
             (do (when value
                   (doseq [sub subs]
                     (>!! sub {:changed? false
                               :value (:v value)
                               :origin id})))
                 (recur (<!! in) subs value))

             ;; TODO: error?
             :else (recur (<!! in) subs value)))
    (Source. in in id true)))

;; TODO: for generic node construction
(defmacro defnode
  [bindings & body]
  `nil)

(core/defn subscribe-input
  [input]
  (let [c (chan)]
    (node-subscribe input c)
    c))

(core/defn subscribe-inputs
  [inputs]
  (map subscribe-input inputs))

;; input nodes = the actual node records
;; inputs = input channels
;; subscribers = atom with list of subscriber channels

;; If the value of the go-loop is false, and messages are received, then they will all be changed.
;; Think about it, as source nodes do not propagate unless they are initialised, all sources their first
;; message will have {:changed? true}. Thus the first messages to arrive at a regular node will all have
;; {:changed true}. These are then propagated further and QED and whatnot.
;; As a result, we do not need to wrap values as we do need to do with sources.
;; Unless... maybe, if sources already start submitting when we are still constructing...
;; TODO: construction finish detection
(core/defn make-map-node
  [input-node f]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (go-loop [in (<!! sub-chan)]
      (match in {:subscribe c} (swap! subscribers #(cons c %)) :else nil)
      (recur (<!! sub-chan)))
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

;; TODO: remove subscribers, it's a sink
(core/defn make-do-apply-node
  [input-nodes action]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        inputs (subscribe-inputs input-nodes)]
    (go-loop [in (<!! sub-chan)]
      (match in {:subscribe c} (swap! subscribers #(cons c %)) :else nil)
      (recur (<!! sub-chan)))
    (go-loop [msgs (map <!! inputs)
              value nil]
      (log/debug (str "do-apply-node " id " has received: " (seq msgs)))
      (let [[changed? v]
            (if (ormap :changed? msgs)
              [true (apply action (map :value msgs))]
              [false value])]
        (doseq [sub @subscribers]
          (>!! sub {:changed? changed? :value v :from id}))
        (recur (map <!! inputs) v)))
    (Node. sub-chan id false)))

;; TODO: more generic approach to node construction
;; TODO: filter should *always* propagate a value...
;; TODO: think about intialisation
(core/defn make-filter-node
  [input-node predicate]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (go-loop [in (<!! sub-chan)]
      (match in {:subscribe c} (swap! subscribers #(cons c %)) :else nil)
      (recur (<!! sub-chan)))
    (go-loop [msg (<!! input)
              pass? false
              value nil]
      (log/debug (str "filter-node " id " has received: " msg))
      (cond (:changed? msg) (let [pass? (predicate (:value msg))]
                              (when pass?
                                (doseq [sub @subscribers]
                                  (>!! sub {:changed? true :value (:value msg) :from id})))
                              (recur (<!! input) pass? value))
            pass? (do (doseq [sub @subscribers]
                        (>!! sub {:changed? false :value value :from id}))
                      (recur (<!! input) pass? value))
            :else (recur (<!! input) pass? value)))
    (Node. sub-chan id false)))

(core/defn make-delay-node
  [input-node]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (go-loop [in (<!! sub-chan)]
      (match in {:subscribe c} (swap! subscribers #(cons c %)) :else nil)
      (recur (<!! sub-chan)))
    (go-loop [msg (<!! input)
              previous-set false
              delayed-set false]
      (log/debug (str "delay-node " id " has received: " msg))
      (let [new-set (:value msg)
            key (:key new-set)]
        (cond
          (and previous-set (:changed? msg))
          (let [new-element (first (ms/minus (:mset new-set) (:mset previous-set)))
                delayed-set
                (if key
                  (if-let [existing (multiset-get previous-set (get new-element key))]
                    (multiset-insert delayed-set existing)
                    delayed-set)
                  previous-set)]
            (doseq [sub @subscribers]
              (>!! sub {:changed? true :value delayed-set :from id}))
            (recur (<!! input) new-set delayed-set))

          previous-set
          (do (doseq [sub @subscribers]
                (>!! sub {:changed? false :value delayed-set :from id}))
              (recur (<!! input) new-set delayed-set))

          :else
          (let [delayed-set (make-multiset key)]
            (doseq [sub @subscribers]
              (>!! sub {:changed? (:changed? msg)       ;; should always be true
                        :value delayed-set
                        :from id}))
            (recur (<!! input) new-set delayed-set)))))
    (Node. sub-chan id false)))

(core/defn make-zip-node
  [left-node right-node]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        inputs (subscribe-inputs [left-node right-node])]
    (go-loop [in (<!! sub-chan)]
      (match in {:subscribe c} (swap! subscribers #(cons c %)) :else nil)
      (recur (<!! sub-chan)))
    (go-loop [msgs (map <!! inputs)
              zipped false]
      (log/debug (str "zip-node " id " has received: " msgs))
      (if (ormap :changed? msgs)
        (let [lset (:value (first msgs))
              rset (:value (second msgs))
              key (:key lset)
              lkvals (map #(get % key) (:mset lset))
              rkvals (map #(get % key) (:mset rset))
              in-both (cs/union (set lkvals) (set rkvals))
              pairs (for [kv in-both
                          :let [lv (multiset-get lset kv)
                                rv (multiset-get rset kv)]
                          :when (and rv lv)]
                      [lv rv])
              zipped (make-multiset false (apply ms/multiset pairs))]
          (doseq [sub @subscribers]
            (>!! sub {:changed? true :value zipped :from id}))
          (recur (map <!! inputs) zipped))
        ;; If nothing is changed, zipped should be initialised
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
    (go-loop [in (<!! sub-chan)]
      (match in {:subscribe c} (swap! subscribers #(cons c %)) :else nil)
      (recur (<!! sub-chan)))
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
    (go-loop [in (<!! sub-chan)]
      (match in {:subscribe c} (swap! subscribers #(cons c %)) :else nil)
      (recur (<!! sub-chan)))
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

;; TODO: the trigger node might cause the whole "the first messages a node receive are changed? true" statement totally FALSE!!!!!!
(core/defn make-throttle-node
  [input-node trigger-node]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        trigger (subscribe-input trigger-node)]
    (go-loop [in (<!! sub-chan)]
      (match in {:subscribe c} (swap! subscribers #(cons c %)) :else nil)
      (recur (<!! sub-chan)))
    (go-loop [msg (<!! input)
              trig (<!! trigger)]
      (log/debug (str "throttle-node " id " has received: " msg))
      (if (:changed? trig)
        (do (doseq [sub @subscribers]
              (>!! sub {:changed? true :value (:value msg) :from id}))
            (recur (<!! input) (<!! trigger)))
        (do (doseq [sub @subscribers]
              (>!! sub {:changed? false :value (:value msg) :from id}))
            (recur (<!! input) (<!! trigger)))))
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
    (thread (loop [set (make-multiset key)]
              (let [v (second (.blpop conn 0 (into-array String [queue])))
                    parsed (edn/read-string v)
                    new-set (multiset-insert set parsed)]
                (>!! (:in *coordinator*) {:destination id :value new-set})
                (recur new-set))))
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
(core/defn throttle
  [signal ms]
  (if (v/signal? signal)
    (let [source-node (make-source-node)
          throttle-node (make-throttle-node (v/value signal) source-node)]
      (>!! (:in *coordinator*) {:new-source (:in source-node)})
      (threadloop []
        (>!! (:in *coordinator*) {:destination (:id source-node) :value true})
        (Thread/sleep ms)
        (recur))
      (v/make-signal throttle-node))
    (throw (Exception. "first argument of throttle must be signal"))))

;; TODO: buffer
(core/defn buffer
  [& args]
  (throw (Exception. "TODO")))

;; TODO: previous (or is this delay? or do latch instead so we can chain?)
(core/defn previous
  [& args]
  (throw (Exception. "TODO")))

(defmacro print-signal
  [signal]
  `(do-apply #(println (str (quote ~signal) ": " %)) ~signal))

(core/defn -main
  [& args]
  (comment (println "") (println "") (println ""))

  (let [r (make-redis "localhost" "testqueue" :key :id)]
    (print-signal r)
    (print-signal (delay r))
    (print-signal (delay (delay r))))

  #_(let [r (make-redis "localhost" "bxlqueue")
        f (filter even? r)
        m1 (map inc f)
        m2 (map2 + f m1)]
    ((lift println) m2))

  #_(let [r1 (make-redis "localhost" "lq")
        r2 (make-redis "localhost" "rq")]
    ((lift println) (map2 + r1 r2)))

  #_(println "Done"))