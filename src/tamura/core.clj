(ns tamura.core
  (:require [clojure.core :as core]
            [clojure.core.async :as a :refer [>!! <!! go go-loop]]
            [clojure.core.match :refer [match]]
            [clojure.data.priority-map :as pm]
            [clojure.edn :as edn]
            [clojure.set :as cs]
            [clojure.string :refer [upper-case]]
            [clojure.tools.logging :as log]
            [clj-time.core :as t]
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


;; TODO: leasing operations in datastructures?

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

(core/defn hash-remove
  [hash key]
  (make-hash (dissoc (:hash hash) key)))

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

(core/defn multiset-remove
  [ms val]
  (-> (:multiset ms)
      (disj val)
      (make-multiset)))

(core/defn multiset-minus
  [l r]
  (-> (ms/minus (:multiset l) (:multiset r))
      (make-multiset)))

(core/defn multiset-union
  [l r]
  (-> (ms/union (:multiset l) (:multiset r))
      (make-multiset)))

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

(declare *coordinator*)

(core/defn- started?
  []
  (let [c (chan 0)
        s (do (>!! (:in *coordinator*) {:started? c})
              (<!! c))]
    (a/close! c)
    s))

(core/defn start
  []
  (>!! (:in *coordinator*) :start))

;; TODO: do we still need heights?
;; TODO: phase2 of multiclock reactive programming (detect when construction is done) (or cheat and Thread/sleep)
(core/defn make-coordinator
  []
  (let [in (chan)]
    (go-loop [msg (<!! in)
              started? false
              sources []]
      (log/debug (str "coordinator received: " msg))
      (match msg
        {:new-source source-chan}
        (if started?
          (throw (Exception. "can not add new sources when already running"))
          (recur (<!! in) started? (cons source-chan sources)))

        {:destination id :value value}
        (do (when started?
              (doseq [source sources]
                (>!! source msg)))
            (recur (<!! in) started? sources))

        {:started? reply-channel}
        (do (>!! reply-channel started?)
            (recur (<!! in) started? sources))

        :start (recur (<!! in) true sources)

        :else (recur (<!! in) started? sources)))
    (Coordinator. in)))

;; NOTE: nodes are currently semi-dynamic; subscribers can be added, but inputs not

;; TODO: implement leasing in the source nodes
;; TODO: priority queue for leasing
;; TODO: timeout must be a period (e.g. t/minutes)
;; TODO: leasing when no data has changed?
;; TODO: ping node to do leasing now and then
(core/defn make-source-node
  [type & {:keys [timeout] :or {timeout false}}]
  (let [in (chan)
        id (new-id!)]
    (go-loop [msg (<!! in)
              subs []
              pm (pm/priority-map)
              value (if (= type ::multiset) (make-multiset) (make-hash))]
      (log/debug (str "source " id " has received: " (seq msg)))
      (match msg
        {:subscribe subscriber}
        (recur (<!! in) (cons subscriber subs) pm value)

        {:destination id :value new-value}
        (let [now (t/now)
              cutoff (when timeout (t/minus now timeout))
              new-coll (if (= type ::multiset)
                         (multiset-insert value new-value)
                         (hash-insert value (first new-value) (second new-value)))
              new-pm (when timeout
                       (assoc pm (if (hash? new-coll) (first new-value) new-value) now))
              [new-coll new-pm]
              (if timeout
                (loop [pm new-pm
                       coll new-coll]
                  (let [[v t] (peek pm)]
                    (if (t/before? t cutoff)
                      (let [coll (if (= type ::multiset)
                                   (multiset-remove coll v)
                                   (hash-remove coll v))
                            pm (pop pm)]
                        (recur pm coll))
                      [coll pm])))
                [new-coll nil])]
          (doseq [sub subs]
            (>!! sub {:changed? true
                      :value    new-coll
                      :origin   id}))
          (recur (<!! in) subs new-pm new-coll))

        {:destination _}
        (do (doseq [sub subs]
              (>!! sub {:changed? false
                        :value    value
                        :origin   id}))
            (recur (<!! in) subs pm value))

        ;; TODO: error?
        :else (recur (<!! in) subs pm value)))
    (Source. in in id true)))

(core/defn subscribe-input
  [input]
  (let [c (chan)]
    (node-subscribe input c)
    c))

(core/defn subscribe-inputs
  [inputs]
  (map subscribe-input inputs))

;; TODO: use this in all nodes
(core/defn send-subscribers
  [subscribers changed? value id]
  (doseq [sub @subscribers]
    (>!! sub {:changed? changed? :value value :from id})))

(defmacro subscriber-loop
  [id channel subscribers]
  `(go-loop [in# (<!! ~channel)]
     (match in#
       {:subscribe c#}
       (do (log/debug (str "node " ~id " has received subscriber message"))
           (if (started?)
             (throw (Exception. "can not add subscribers to nodes when started"))
             (swap! ~subscribers #(cons c# %))))

       :else nil)
     (recur (<!! ~channel))))

;; TODO: for generic node construction
(defmacro defnode
  [name inputs bindings & body]
  `(throw (Exception. "TODO")))

;; input nodes = the actual node records
;; inputs = input channels
;; subscribers = atom with list of subscriber channels

;; TODO: instead of checking if one or more inputs have changed, also check that the value for each input is sane (i.e. a multiset)
;; The rationale is that a node can only produce a :changed? true value iff all its inputs have been true at least once.
;; Possible solution: an inputs-seen flag of some sorts?

;; TODO: construction finish detection
;; TODO: test
(core/defn make-map-hash-node
  [input-node f]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
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
                                          values))]
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
    (subscriber-loop id sub-chan subscribers)
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
;; TODO: re-test
(core/defn make-filter-node
  [input-node predicate]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<!! input)
              value nil]
      (log/debug (str "filter-node " id " has received: " msg))
      (if (:changed? msg)
        (let [values (if (hash? (:value msg))
                       (:hash (:value msg))
                       (:multiset (:value msg)))
              filtered (filter predicate values)
              new (if (hash? (:value msg))
                    (make-hash (into {} filtered))
                    (make-multiset (apply ms/multiset filtered)))]
          (doseq [sub @subscribers]
            (>!! sub {:changed? true :value new :from id}))
          (recur (<!! input) new))
        (do (doseq [sub @subscribers]
              (>!! sub {:changed? false :value value :from id}))
            (recur (<!! input) value))))
    (Node. sub-chan id false)))

;; TODO: put in docstring that it emits empty hash or set
;; TODO: make sure it still works with leasing

;; TODO: define similar semantics for buffers
(comment
  "Semantics delay, multiset, after leasing/buffer"
  #{}         => #{}
  #{1}        => #{}
  #{1 2}      => #{1}
  #{1 2 3}    => #{1 2}
  #{2 3 4}    => #{2 3}
  #{2 3 4 5}  => #{2 3 4}

  "Semantics delay, hash, after leasing/buffer"
  {}                => {}
  {:a 1}            => {}
  {:a 1 :b 1}       => {}
  {:a 2 :b 1}       => {:a 1}
  {:a 2 :b 2}       => {:a 1 :b 1}
  {:b 2 :c 1}       => {:b 1}
  {:b 2 :c 1 :a 3}  => {:b 1})

(core/defn make-delay-node
  [input-node]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<!! input)
              previous (set nil)
              delayed nil]
      (log/debug (str "delay-node " id " has received: " msg))
      (cond (and (:changed? msg) (multiset? (:value msg)))
            (let [delayed (or delayed (make-multiset))
                  added (multiset-minus (:value msg) delayed)
                  removed (multiset-minus delayed (:value msg))
                  delayed (if removed (multiset-minus delayed removed) delayed)]
              (doseq [sub @subscribers]
                (>!! sub {:changed? true :value delayed :from id}))
              (recur (<!! input) nil (multiset-union delayed added)))

            (:changed? msg)
            (let [previous-keys (set (hash-keys previous))
                  new-keys (set (hash-keys (:value msg)))
                  removed-keys (cs/difference previous-keys new-keys)
                  new-set (hash->set (:value msg))
                  previous-set (hash->set previous)
                  new-element (first (cs/difference new-set previous-set))
                  delayed (if-let [existing (hash-get previous (first new-element))]
                            (hash-insert delayed (first new-element) existing)
                            (or delayed (make-hash)))
                  delayed (reduce #(hash-remove %1 %2) delayed removed-keys)]
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
    (subscriber-loop id sub-chan subscribers)
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

;; TODO: assert input is a multiset
;; TODO: output as a hash?
(core/defn make-multiplicities-node
  [input-node]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<!! input)
              value nil]
      (log/debug (str "multiplicities-node " id " has received: " msg))
      (if (:changed? msg)
        (let [multiplicities (multiset-multiplicities (:value msg))
              new-set (make-multiset (apply ms/multiset (seq multiplicities)))]
          (doseq [sub @subscribers]
            (>!! sub {:changed? true :value new-set :from id}))
          (recur (<!! input) new-set))
        (do (doseq [sub @subscribers]
              (>!! sub {:changed? false :value value :from id}))
            (recur (<!! input) value))))
    (Node. sub-chan id false)))

(core/defn make-reduce-node
  [input-node f & initial]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<!! input)
              value (make-multiset)]
      (log/debug (str "reduce-node " id " has received: " msg))
      (if (:changed? msg)
        (let [values (or (:hash (:value msg)) (:multiset (:value msg)))
              reduced (if initial
                        (if (>= (count values) 1) (reduce f (first initial) values) nil)
                        (if (>= (count values) 2) (reduce f values) nil))
              new-set (make-multiset (ms/multiset reduced))]
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
;; TODO: make throttle that propages true on every tick AND one that only propagates true if something has changed since
(core/defn make-throttle-node
  [input-node trigger-node]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        trigger (subscribe-input trigger-node)]
    (subscriber-loop id sub-chan subscribers)
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

(comment
  "Semantics buffer of size 2, multiset, after leasing/buffer"
  #{}         => #{}
  #{1}        => #{1}
  #{1 2}      => #{1 2}
  #{1 2 3}    => #{2 3}
  #{4}        => #{4}
  #{4 5}      => #{4 5}

  "Semantics buffer of size 2, hash, after leasing/buffer"
  {}                => {}
  {:a 1}            => {:a 1}
  {:a 1 :b 1}       => {:a 1 :b 1}
  {:a 1 :b 1 :c 1}  => {:b 1 :c 1}
  {:d 1}            => {:d 1}
  {:d 1 :e 1}       => {:d 1 :e 1})

;; TODO: deal with removals in chained buffers and so on (also, leasing)
(core/defn make-buffer-node
  [input-node size]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<!! input)
              previous nil
              buffer-list (LinkedList.)
              buffer nil]
      (log/debug (str "buffer-node " id " has received: " msg))
      (cond (and (:changed? msg) (multiset? (:value msg)))
            (let [new-element (if previous
                                (first (ms/minus (:multiset (:value msg)) previous))
                                (first (:multiset (:value msg))))
                  removed (if previous
                            (ms/minus previous (:multiset (:value msg)))
                            (ms/multiset))]
              (when-not (empty? removed)
                (doseq [el removed]
                  (.remove buffer-list el)))
              (when (= (count buffer-list) size)
                (.removeLast buffer-list))
              ;; TODO: more efficient, thus without the apply thing
              (if new-element
                (do (.addFirst buffer-list new-element)
                    (let [buffer (make-multiset (apply ms/multiset buffer-list))]
                      (send-subscribers subscribers true buffer id)
                      (recur (<!! input) (:multiset (:value msg)) buffer-list buffer)))
                (do (send-subscribers subscribers true buffer id)
                    (recur (<!! input) (:multiset (make-multiset)) buffer-list buffer))))

            (:changed? msg)
            (let [new-set (hash->set (:value msg))
                  previous-set (hash->set previous)
                  new-element (first (cs/difference new-set previous-set))
                  new-key  (first new-element)
                  previous-keys (set (hash-keys previous))
                  new-keys (set (hash-keys (:value msg)))
                  removed-keys (cs/difference previous-keys new-keys)
                  buffer (if (empty? removed-keys)
                           buffer
                           (reduce (fn [buffer key]
                                     (.remove buffer-list key)
                                     (hash-remove buffer key))
                                   buffer
                                   removed-keys))]
              (if (hash-contains? buffer new-key)
                (let [buffer (hash-insert buffer new-key (second new-element))]
                  ;; NOTE: use (.indexOf buffer-list new-key) instead?
                  (.remove buffer-list new-key)
                  (.addFirst buffer-list new-key)
                  (send-subscribers subscribers true buffer id)
                  (recur (<!! input) (:value msg) buffer-list buffer))
                (let [buffer (-> (or buffer (make-hash))
                                 (#(if (= (count buffer-list) size)
                                    (hash-remove % (.removeLast buffer-list))
                                    %))
                                 (hash-insert new-key (second new-element)))]
                  (.addFirst buffer-list new-key)
                  (send-subscribers subscribers true buffer id)
                  (recur (<!! input) (:value msg) buffer-list buffer))))

            :else
            (do (doseq [sub @subscribers]
                  (>!! sub {:changed? false :value buffer :from id}))
                (recur (<!! input) previous buffer-list buffer))))
    (Node. sub-chan id false)))

(def ^:dynamic ^:static *coordinator* (make-coordinator))

(core/defn- make-signal
  [node]
  ;; NOTE: this is a little hack to ensure correct ordering of start/started? messages
  ;; We make sure that every node sends at least one message to the coordinator, so the coordinator is not started
  ;; before graph construction is finished.
  (>!! (:in *coordinator*) :else)
  (v/make-signal node))

;; TODO: redis sink
;; TODO: put coordinator in Node/Source record? *coordinator* is dynamic...
;; TODO: something something polling time
;; TODO: error if key not present
(core/defn make-redis
  [host queue & {:keys [key] :or {key false}}]
  (let [node (make-source-node (if key ::hash ::multiset))
        id (:id node)
        pool (JedisPool. host)
        conn (.getResource pool)]
    (>!! (:in *coordinator*) {:new-source (:in node)})
    (threadloop [values (if key (make-hash) (make-multiset))]
      (let [v (second (.blpop conn 0 (into-array String [queue])))
            parsed (edn/read-string v)
            value (if key
                    [(get parsed key) (dissoc parsed key)]
                    parsed)]
        (>!! (:in *coordinator*) {:destination id :value value})
        (recur values)))
    (make-signal node)))

;; TODO: maybe rename to redis-input
(def redis make-redis)

;; For incremental etc: keep dict of input => output for elements
;; TODO: what if function changes key?
;; Possible solution: prevent by comparing in and output, and make keychanges possible by special map function
;; TODO: test
(core/defn map-to-hash
  [f arg]
  (if (v/signal? arg)
    (let [source-node (v/value arg)
          node (make-map-hash-node source-node f)]
      (make-signal node))
    (throw (Exception. "map-to-hash requires a signal as second argument"))))

;; TODO: test
(core/defn map-to-multiset
  [f arg]
  (if (v/signal? arg)
    (let [source-node (v/value arg)
          node (make-map-multiset-node source-node f)]
      (make-signal node))
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
    (make-signal (make-do-apply-node (map v/value (cons arg args)) f))
    (throw (Exception. "do-apply needs to be applied to signals"))))

(core/defn filter
  [f arg]
  (if (v/signal? arg)
    (let [source-node (v/value arg)
          node (make-filter-node source-node f)]
      (make-signal node))
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
      (make-signal node))
    (throw (Exception. "argument to delay should be a signal"))))

(core/defn zip
  [left right]
  (if (and (v/signal? left) (v/signal? right))
    (make-signal (make-zip-node (v/value left) (v/value right)))
    (throw (Exception. "arguments to zip should be signals"))))

(core/defn multiplicities
  [arg]
  (if (v/signal? arg)
    (make-signal (make-multiplicities-node (v/value arg)))
    (throw (Exception. "argument to delay should be a signal"))))

(core/defn reduce
  ([f arg]
   (if (v/signal? arg)
     (-> (v/value arg)
         (make-reduce-node f)
         (make-signal))
     (core/reduce f arg)))
  ([f val coll]
   (if (v/signal? coll)
     (-> (v/value coll)
         (make-reduce-node f val)
         (make-signal))
     (core/reduce f val coll))))

;; We *must* work with a node that signals the coordinator to ensure correct propagation in situations where
;; nodes depend on a throttle signal and one or more other signals.
;; TODO: something something initialisation?
;; TODO: what if the input hasn't changed next time we trigger? Do we still propagate a "change"?
(core/defn throttle
  [signal ms]
  (if (v/signal? signal)
    (let [source-node (make-source-node ::multiset)
          throttle-node (make-throttle-node (v/value signal) source-node)]
      (>!! (:in *coordinator*) {:new-source (:in source-node)})
      (threadloop []
        (Thread/sleep ms)
        (>!! (:in *coordinator*) {:destination (:id source-node) :value nil})
        (recur))
      (make-signal throttle-node))
    (throw (Exception. "first argument of throttle must be signal"))))

;; TODO: corner case size 0
(core/defn buffer
  [sig size]
  (if (v/signal? sig)
    (let [source-node (v/value sig)
          node (make-buffer-node source-node size)]
      (make-signal node))
    (throw (Exception. "first argument to delay should be a signal"))))

;; TODO: previous (or is this delay? or do latch instead so we can chain?)
(core/defn previous
  [& args]
  (throw (Exception. "TODO")))

(defmacro print-signal
  [signal]
  `(do-apply #(locking *out* (println (quote ~signal) ": " %)) ~signal))

(core/defn -main
  [& args]
  (let [rh (make-redis "localhost" "tqh" :key :id)
        delayed (delay rh)
        zipped (zip rh delayed)
        buffered (buffer rh 3)]
    ;(print-signal rh)
    ;(print-signal delayed)
    ;(print-signal zipped)
    (print-signal buffered)
    )

  (let [rms (make-redis "localhost" "tqms")
        delayed (delay rms)
        buffered (buffer rms 3)]
    ;(print-signal rms)
    ;(print-signal delayed)
    (print-signal buffered))

  (println "Ready"))