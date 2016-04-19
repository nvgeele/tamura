(ns tamura.core
  (:require [clojure.core :as core]
            [clojure.core.async
             :as a
             :refer [>! <! >!! <!! go buffer close! thread alts! alts!! timeout go-loop]]
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

(defprotocol Reactive
  (value [this])
  (subscribe [this reactor])
  (height [this]))

;; TODO: update height
(defprotocol Producer
  (tick [this]))

(defprotocol Reactor
  (update [this tick value]))

;; TODO: use threadpool?
(core/defn- make-producer-thread
  [producer rate]
  (let [t (Thread.
            (fn [] (loop []
                     (tick producer)
                     (Thread/sleep rate)
                     (recur))))]
    (.start t)
    t))

(core/defn- update-subscribers
  [subscribers tick value]
  (doseq [sub subscribers]
    (update sub tick value)))

;; f should receive boolean, not the value, as the value produced could be false.
;; therefore f is responsible for swapping the new value in etc.
(deftype FunctionProducer
  [f state]
  Producer
  (tick [this]
    (when (f state)
      (update-subscribers (:subscribers @state) nil (:value @state)))))

;; f *must* produce a new value
(deftype FunctionReactor
  [f state]
  Reactor
  (update [this tick value]
    (let [v (f tick value)]
      (swap! state assoc :value v)
      (update-subscribers (:subscribers @state) nil (:value @state)))))

(extend-protocol Reactive
  FunctionProducer
  (value [this]
    (:value @(.state this)))
  (subscribe [this reactor]
    (swap! (.state this) #(assoc % :subscribers (cons reactor (:subscribers %)))))
  (height [this]
    (:height @(.state this)))

  FunctionReactor
  (value [this]
    (:value @(.state this)))
  (subscribe [this reactor]
    (swap! (.state this) #(assoc % :subscribers (cons reactor (:subscribers %)))))
  (height [this]
    (:height @(.state this))))

(core/defn make-redis
  [host key]
  (let [pool (JedisPool. host)
        state {:pool pool
               :conn (.getResource pool)
               :key key
               :subscribers []
               :value nil
               :height 0}
        f (fn [state]
            (when-let [v (.rpop (:conn @state) (:key @state))]
              (swap! state assoc :value (edn/read-string v))
              (update-subscribers (:subscribers @state) nil (:value @state))))
        producer (new FunctionProducer f (atom state))]
    (make-producer-thread producer 10)
    (v/make-eventstream producer)))

(def redis make-redis)

(core/defn map
  [f arg]
  (if (v/eventstream? arg)
    (let [prod (v/value arg)
          ph (height prod)
          state (atom {:subscribers [] :value nil :height (inc ph)})
          mf (fn [tick value] (f value))
          reactor (new FunctionReactor mf state)]
      (subscribe prod reactor)
      (v/make-eventstream reactor))
    (core/map f arg)))

(core/defn lift
  [f]
  (fn [arg]
    (if (v/eventstream? arg)
      (map f arg)
      (throw (Exception. "Argument for lifted function should be an event stream")))))

;; intra-actor message: {:changed? false :value nil :origin nil :destination nil}
;; each actor counts how many updates it receives, if updates = parents, then proceed


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

;; TODO: phase2 of multiclock reactive programming (detect when construction is done)
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

(core/defn ormap
  [f lst]
  (loop [l lst]
    (cond
      (empty? l) false
      (f (first l)) true
      :else (recur (rest l)))))

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

(core/defn -main
  [& args]
  (println "") (println "") (println "")
  (let [c (:in (make-coordinator))
        s1 (make-source-node)
        s2 (make-source-node)
        p1 (make-node [s1 s2] (comp upper-case str))
        p2 (make-node [p1] println)]
    (>!! c {:new-source (:in s1)})
    (>!! c {:new-source (:in s2)})
    (>!! c {:destination (:id s1) :value 'kaka})
    (>!! c {:destination (:id s2) :value 'pipi})
    (Thread/sleep 3000)
    (println "Done")))