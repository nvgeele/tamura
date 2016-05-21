(ns tamura.core
  (:require [clojure.core :as core]
            [clojure.core.async :as a :refer [>!! >! <!! <! go go-loop]]
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
  (:use [tamura.datastructures])
  (:import [redis.clients.jedis Jedis JedisPool]
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
(def node-constructors (atom {}))

(core/defn reset-graph!
  []
  (swap! nodes (constantly {}))
  (swap! sources (constantly []))
  (swap! counter (constantly 0)))

(core/defn register-node!
  [node]
  (let [id (new-id!)]
    (swap! nodes assoc id node)
    (doseq [input-id (:inputs node)]
      (swap! nodes update-in [input-id :outputs] conj id))
    id))

(core/defn register-source!
  [node]
  (let [id (new-id!)]
    (swap! sources conj id)
    (swap! nodes assoc id node)
    id))

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
            inputs (map #(:node (get @nodes %)) (:inputs node))
            node-obj ((get @node-constructors (:node-type node)) id (:args node) inputs)]
        (swap! nodes update-in [id :node] (constantly node-obj))
        (if (= (:node-type node) ::source)
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

(core/defn start
  []
  (when (started?)
    (throw (Exception. "already started")))
  (build-nodes!)
  ;; TODO: remove the sleep...
  (Thread/sleep 1000)
  (>!! (:in *coordinator*) :start))

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

        :else (recur (<! in) started? sources)))
    (Coordinator. in)))

;; NOTE: nodes are currently semi-dynamic; subscribers can be added, but inputs not

(core/defn subscribe-input
  [input]
  (let [c (chan)]
    (node-subscribe input c)
    c))

(core/defn subscribe-inputs
  [inputs]
  (map subscribe-input inputs))

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

;; input nodes = the actual node records
;; inputs = input channels
;; subscribers = atom with list of subscriber channels

;; TODO: instead of checking if one or more inputs have changed, also check that the value for each input is sane (i.e. a multiset)
;; The rationale is that a node can only produce a :changed? true value iff all its inputs have been true at least once.
;; Possible solution: an inputs-seen flag of some sorts?

;; TODO: construction finish detection
;; TODO: test
(core/defn make-map-hash-node
  [id [f] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        selector (if (= (:return-type input-node) :hash) :hash :multiset)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (make-hash)]
      (log/debug (str "map-to-hash-node " id " has received: " msg))
      (if (:changed? msg)
        (let [values (selector (:value msg))
              new-hash (make-hash (reduce (fn [h v]
                                            (let [[k v] (f v)]
                                              (assoc h k v)))
                                          {}
                                          values))]
          (doseq [sub @subscribers]
            (>! sub {:changed? true :value new-hash :from id}))
          (recur (<! input) new-hash))
        (do (doseq [sub @subscribers]
              (>! sub {:changed? false :value value :from id}))
            (recur (<! input) value))))
    (Node. id ::map-to-hash :hash sub-chan)))
(register-constructor! ::map-to-hash make-map-hash-node)

;; TODO: tests
(core/defn make-map-multiset-node
  [id [f] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        selector (if (= (:return-type input-node) :hash) :hash :multiset)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (make-multiset)]
      (log/debug (str "map-to-multiset-node " id " has received: " msg))
      (if (:changed? msg)
        (let [values (selector (:value msg))
              new-set (make-multiset (apply ms/multiset (map f values)))]
          (doseq [sub @subscribers]
            (>! sub {:changed? true :value new-set :from id}))
          (recur (<! input) new-set))
        (do (doseq [sub @subscribers]
              (>! sub {:changed? false :value value :from id}))
            (recur (<! input) value))))
    (Node. id ::map-to-multiset :multiset sub-chan)))
(register-constructor! ::map-to-multiset make-map-multiset-node)

;; TODO: tests
(core/defn make-do-apply-node
  [id [action] input-nodes]
  (let [inputs (subscribe-inputs input-nodes)
        selectors (map #(if (= (:return-type %) :hash) :hash :multiset) input-nodes)]
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
    (Sink. id ::do-apply)))
(register-constructor! ::do-apply make-do-apply-node)

;; TODO: say false when no new element is added to the filtered set?
;; TODO: tests
(core/defn make-filter-node
  [id [predicate] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        input-type (:return-type input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value nil]
      (log/debug (str "filter-node " id " has received: " msg))
      (if (:changed? msg)
        (let [values (if (= input-type :hash)
                       (:hash (:value msg))
                       (:multiset (:value msg)))
              filtered (filter predicate values)
              new (if (= input-type :hash)
                    (make-hash (into {} filtered))
                    (make-multiset (apply ms/multiset filtered)))]
          (doseq [sub @subscribers]
            (>! sub {:changed? true :value new :from id}))
          (recur (<! input) new))
        (do (doseq [sub @subscribers]
              (>! sub {:changed? false :value value :from id}))
            (recur (<! input) value))))
    (Node. id ::filter (:return-type input-node) sub-chan)))
(register-constructor! ::filter make-filter-node)

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

(core/defn make-zip-node
  [id [] [left-node right-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        left-in (subscribe-input left-node)
        right-in (subscribe-input right-node)]
    (when-not (and (= (:return-type left-node) :hash)
                   (= (:return-type right-node) :hash))
      (throw (Exception. "inputs to zip must have hashes as return types")))
    (subscriber-loop id sub-chan subscribers)
    (go-loop [l (<! left-in)
              r (<! right-in)
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
            (>! sub {:changed? true :value zipped :from id}))
          (recur (<! left-in) (<! right-in) zipped))
        (do (doseq [sub @subscribers]
              (>! sub {:changed? false :value zipped :from id}))
            (recur (<! left-in) (<! right-in) zipped))))
    (Node. id ::zip :hash sub-chan)))
(register-constructor! ::zip make-zip-node)

;; TODO: output as a hash?
;; TODO: test
(core/defn make-multiplicities-node
  [id [] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (when-not (= (:return-type input-node) :multiset)
      (throw (Exception. "input to multiplicities must have multiset as return type")))
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value nil]
      (log/debug (str "multiplicities-node " id " has received: " msg))
      (if (:changed? msg)
        (let [multiplicities (multiset-multiplicities (:value msg))
              new-set (make-multiset (apply ms/multiset (seq multiplicities)))]
          (doseq [sub @subscribers]
            (>! sub {:changed? true :value new-set :from id}))
          (recur (<! input) new-set))
        (do (doseq [sub @subscribers]
              (>! sub {:changed? false :value value :from id}))
            (recur (<! input) value))))
    (Node. id ::multiplicities :multiset sub-chan)))
(register-constructor! ::multiplicities make-multiplicities-node)

;; TODO: test
(core/defn make-reduce-node
  ;;[id input-node f & initial]
  [id [f & initial] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (make-multiset)]
      (log/debug (str "reduce-node " id " has received: " msg))
      (if (:changed? msg)
        (let [values (or (:hash (:value msg)) (:multiset (:value msg)))
              reduced (if initial
                        (if (>= (count values) 1) (reduce f (first initial) values) nil)
                        (if (>= (count values) 2) (reduce f values) nil))
              new-set (make-multiset (ms/multiset reduced))]
          (doseq [sub @subscribers]
            (>! sub {:changed? true :value new-set :from id}))
          (recur (<! input) new-set))
        (do (doseq [sub @subscribers]
              (>! sub {:changed? false :value value :from id}))
            (recur (<! input) value))))
    (Node. id ::reduce :multiset sub-chan)))
(register-constructor! ::reduce make-reduce-node)

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

(comment "Semantics buffer (size 2)"
  "multiset"
  #{1}       => #{1}
  #{1 2}     => #{1 2}
  #{1 2 3}   => #{2 3}
  #{1 2 3 4} => #{3 4}

  "hash"
  {:a #{1}}       => {:a #{1}}
  {:a #{1 2}}     => {:a #{1 2}}
  {:a #{1 2 3}}   => {:a #{2 3}}
  {:a #{1 2 3 4}} => {:a #{3 4}})

(comment
  "Semantics buffer of size 2, multiset, after leasing/buffer"
  #{1}        => #{1}
  #{1 2}      => #{1 2}
  #{1 2 3}    => #{2 3}
  - timeout -
  #{4}        => #{4}
  #{4 5}      => #{4 5}

  "Semantics buffer of size 3, multiset, after buffered source of size 2"
  #{1}   => #{1}
  #{1 2} => #{1 2}
  #{2 3} => #{2 3}
  #{3 4} => #{3 4}

  "Semantics buffer of size 2, hash, after leasing/buffer"
  {:a #{1}}             => {:a #{1}}
  {:a #{1 2}}           => {:a #{1 2}}
  {:a #{1 2 3}}         => {:a #{2 3}}
  {:a #{1 2 3} :b #{1}} => {:a #{2 3} :b #{1}}
  - timeout -
  {:d #{1}}             => {:d #{1}}
  {:d #{1} :e #{1}}     => {:d #{1} :e #{1}})

;; TODO: deal with removals in chained buffers and so on (also, leasing)

(comment
  (let [new-set (hash->set (:value msg))
        previous-set (hash->set previous)
        new-element (first (cs/difference new-set previous-set))
        new-key (first new-element)
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
        (send-subscribers @subscribers true buffer id)
        (recur (<! input) (:value msg) buffer-list buffer))
      (let [buffer (-> (or buffer (make-hash))
                       (#(if (= (count buffer-list) size)
                          (hash-remove % (.removeLast buffer-list))
                          %))
                       (hash-insert new-key (second new-element)))]
        (.addFirst buffer-list new-key)
        (send-subscribers @subscribers true buffer id)
        (recur (<! input) (:value msg) buffer-list buffer)))))

(comment
  (let [new-element (first (to-multiset
                             (if previous
                               (multiset-minus (:value msg) previous)
                               (:value msg))))
        removed (if previous
                  (multiset-minus previous (:value msg))
                  (make-multiset))
        buffer (multiset-insert
                 (if (multiset-empty? removed)
                   buffer
                   (reduce #(multiset-remove %1 %2) buffer (to-multiset removed)))
                 new-element)]
    (send-subscribers @subscribers true (to-regular-multiset buffer) id)
    (recur (<! input) (:value msg) buffer)))

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
                  inserted (first (hash-inserted (:value msg)))
                  buffer (hash-insert (reduce #(hash-remove-element %1 (first %2) (second %2)) buffer removed)
                                      (first inserted) (second inserted))]
              (send-subscribers @subscribers true (to-regular-hash buffer) id)
              (recur (<! input) (:value msg) buffer))

            (:changed? msg)
            (let [new (first (multiset-inserted (:value msg)))
                  removed (multiset-removed (:value msg))
                  buffer (multiset-insert (reduce #(multiset-remove %1 %2) buffer removed) new)]
              (send-subscribers @subscribers true (to-regular-multiset buffer) id)
              (recur (<! input) (:value msg) buffer))

            :else
            (do (send-subscribers @subscribers false buffer id)
                (recur (<! input) previous buffer))))
    (Node. id ::buffer (:return-type input-node) sub-chan)))
(register-constructor! ::buffer make-buffer-node)

(def ^:dynamic ^:private *coordinator* (make-coordinator))

(core/defn- make-signal
  [node]
  (v/make-signal node))

;; TODO: redis sink
;; TODO: put coordinator in Node/Source record? *coordinator* is dynamic...
;; TODO: something something polling time
;; TODO: error if key not present
;; TODO: maybe rename to redis-input
(core/defn make-redis
  [host queue & {:keys [key] :or {key false}}]
  (let [node {:node-type ::source :args [(if key :hash :multiset) :key key]}
        id (register-source! node)
        conn (Jedis. host)]
    (threadloop [values (if key (make-hash) (make-multiset))]
      (let [v (second (.blpop conn 0 (into-array String [queue])))
            parsed (edn/read-string v)
            value (if key
                    [(get parsed key) (dissoc parsed key)]
                    parsed)]
        (>!! (:in *coordinator*) {:destination id :value value})
        (recur values)))
    (make-signal id)))
(def redis make-redis)

;; For incremental etc: keep dict of input => output for elements
;; TODO: what if function changes key?
;; Possible solution: prevent by comparing in and output, and make keychanges possible by special map function
;; TODO: test
(core/defn map-to-hash
  [f arg]
  (if (v/signal? arg)
    (let [input (v/value arg)
          node {:node-type ::map-to-hash :args [f] :inputs [input]}
          id (register-node! node)]
      (make-signal id))
    (throw (Exception. "map-to-hash requires a signal as second argument"))))

;; TODO: test
(core/defn map-to-multiset
  [f arg]
  (if (v/signal? arg)
    (let [input (v/value arg)
          node (register-node! {:node-type ::map-to-multiset :args [f] :inputs [input]})]
      (make-signal node))
    (throw (Exception. "map-to-multiset requires a signal as second argument"))))

;; TODO: make this a sink
(core/defn do-apply
  [f arg & args]
  (if (andmap v/signal? (cons arg args))
    (let [inputs (map v/value (cons arg args))
          node (register-node! {:node-type ::do-apply :args [f] :inputs inputs})]
      (make-signal node))
    (throw (Exception. "do-apply needs to be applied to signals"))))

(core/defn filter
  [f arg]
  (if (v/signal? arg)
    (let [input (v/value arg)
          node (register-node! {:node-type ::filter :args [f] :inputs [input]})]
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
    (let [input (v/value arg)
          node (register-node! {:node-type ::delay :args [] :inputs [input]})]
      (make-signal node))
    (throw (Exception. "argument to delay should be a signal"))))

(core/defn zip
  [left right]
  (if (and (v/signal? left) (v/signal? right))
    (let [left (v/value left)
          right (v/value right)
          node (register-node! {:node-type ::zip :args [] :inputs [left right]})]
      (make-signal node))
    (throw (Exception. "arguments to zip should be signals"))))

(core/defn multiplicities
  [arg]
  (if (v/signal? arg)
    (let [input (v/value arg)
          node (register-node! {:node-type ::multiplicities :args [] :inputs [input]})]
      (make-signal node))
    (throw (Exception. "argument to delay should be a signal"))))

(core/defn reduce
  ([f arg]
   (if (v/signal? arg)
     (make-signal (register-node! {:node-type ::reduce :args [f] :inputs [(v/value arg)]}))
     (core/reduce f arg)))
  ([f val coll]
   (if (v/signal? coll)
     (make-signal (register-node! {:node-type ::reduce :args [f val] :inputs [(v/value coll)]}))
     (core/reduce f val coll))))

;; We *must* work with a node that signals the coordinator to ensure correct propagation in situations where
;; nodes depend on a throttle signal and one or more other signals.
;; TODO: something something initialisation?
;; TODO: what if the input hasn't changed next time we trigger? Do we still propagate a "change"?
(core/defn throttle
  [signal ms]
  (if (v/signal? signal)
    (let [trigger (register-source! {:node-type ::source :args [:multiset]})
          node (register-node! {:node-type ::throttle :args [ms] :inputs [(v/value signal) trigger]})]
      (threadloop []
        (Thread/sleep ms)
        (>!! (:in *coordinator*) {:destination trigger :value nil})
        (recur))
      (make-signal node))
    (throw (Exception. "first argument of throttle must be signal"))))

;; TODO: corner case size 0
(core/defn buffer
  [sig size]
  (if (v/signal? sig)
    (make-signal (register-source! {:node-type ::buffer :args [size] :inputs [(v/value sig)]}))
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