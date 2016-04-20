(ns tamura.core
  (:require [clojure.core :as core]
            [clojure.core.async :as a :refer [>!! <!! go go-loop]]
            [clojure.core.match :refer [match]]
            [clojure.edn :as edn]
            [clojure.string :refer [upper-case]]
            [clojure.tools.logging :as log]
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

(core/defn ormap
  [f lst]
  (loop [l lst]
    (cond
      (empty? l) false
      (f (first l)) true
      :else (recur (rest l)))))

(defmacro thread
  [& body]
  `(doto (Thread. (fn [] ~@body))
     (.start)))

(defrecord Coordinator [in])
(defrecord Node [sub-chan id source?])
(defrecord Source [in sub-chan id source?])                 ;; isa Node

(def buffer-size 32)
(defmacro chan
  ([] `(a/chan ~buffer-size))
  ([size] `(a/chan ~size)))
(def counter (atom 0))
(core/defn new-id!
  []
  (swap! counter inc))

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

(defmacro node-subscribe
  [source channel]
  `(>!! (:sub-chan ~source) {:subscribe ~channel}))

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
(core/defn make-node
  [input-nodes action]
  (let [id (new-id!)
        sub-chan (chan)
        subscribers (atom [])
        inputs (for [node input-nodes]
                 (let [c (chan)]
                   (node-subscribe node c)
                   c))]
    (go-loop [in (<!! sub-chan)]
      (match in {:subscribe c} (swap! subscribers #(cons c %)) :else nil)
      (recur (<!! sub-chan)))
    (go-loop [msgs (map <!! inputs)
              value nil]
      (log/debug (str "node " id " has received: " (seq msgs)))
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
        input (let [c (chan)]
                (node-subscribe input-node c)
                c)]
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

;; TODO: for generic node construction
(defmacro defnode
  [bindings & body]
  `nil)

(def ^:dynamic *coordinator* (make-coordinator))

;; TODO: put coordinator in Node/Source record? *coordinator* is dynamic...
(core/defn make-redis
  [host key]
  (let [node (make-source-node)
        id (:id node)
        pool (JedisPool. host)
        conn (.getResource pool)]
    (>!! (:in *coordinator*) {:new-source (:in node)})
    (thread (loop [v (.rpop conn key)]
              (when v
                (>!! (:in *coordinator*) {:destination id :value (edn/read-string v)}))
              (Thread/sleep 10)
              (recur (.rpop conn key))))
    (v/make-signal node)))

(def redis make-redis)

(core/defn filter
  [f arg]
  (if (v/signal? arg)
    (let [source-node (v/value arg)
          node (make-filter-node source-node f)]
      (v/make-signal node))
    (core/filter f arg)))

(core/defn map
  [f arg]
  (if (v/signal? arg)
    (let [source-node (v/value arg)
          node (make-node [source-node] f)]
      (v/make-signal node))
    (core/map f arg)))

(core/defn map2
  [f arg1 arg2]
  (if (and (v/signal? arg1) (v/signal? arg2))
    (let [l (v/value arg1)
          r (v/value arg2)
          node (make-node [l r] f)]
      (v/make-signal node))
    (throw (Exception. "map2 takes 2 signals"))))

(core/defn lift
  [f]
  (fn [arg]
    (if (v/signal? arg)
      (map f arg)
      (throw (Exception. "Argument for lifted function should be a signal")))))

(core/defn -main
  [& args]
  (println "") (println "") (println "")
  (let [r (make-redis "localhost" "bxlqueue")
        f (filter even? r)
        m1 (map inc f)
        m2 (map2 + f m1)]
    ((lift println) m2))
  (let [r1 (make-redis "localhost" "lq")
        r2 (make-redis "localhost" "rq")]
    ((lift println) (map2 + r1 r2)))
  (println "Done"))