(ns examples.sparky
  (:require [clojure.core.async :as a :refer [>!! >! <!! <! go go-loop]]
            [clojure.core.match :refer [match]]
            [clojure.pprint :refer [pprint]]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [clj-time.format :as ftime]
            [clj-time.core :as time]
            [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [flambo.streaming :as fs]
            [flambo.function :as fn]
            [multiset.core :as ms])
  (:use [tamura.datastructures])
  (:import [org.apache.spark.api.java JavaSparkContext]
           [redis.clients.jedis Jedis]
           [scala Tuple2])
  (:gen-class))

;;;; SET-UP & CONFIG ;;;;

(declare ^:private ^JavaSparkContext sc)

(declare ^:dynamic ^:private *coordinator*)

(defn setup-spark!
  [& [config]]
  (alter-var-root
    (var sc)
    (fn [& args]
      (-> (conf/spark-conf)
          (conf/app-name (get :app-name config "tamura-app"))
          (conf/master (get :master config "local[*]"))
          (f/spark-context)))))

;;;; HELPERS ;;;;

(def ^:private print-lock (Object.))

(defn pprint*
  [& args]
  (locking print-lock
    (apply pprint args)))

(defn println*
  [& args]
  (locking print-lock
    (apply println args)))

(defn emptyRDD
  []
  (.emptyRDD sc))

(defn ormap
  [f lst]
  (loop [l lst]
    (cond
      (empty? l) false
      (f (first l)) true
      :else (recur (rest l)))))

(def buffer-size 32)
(defmacro chan
  ([] `(a/chan ~buffer-size))
  ([size] `(a/chan ~size)))
(def counter (atom 0))
(defn new-id!
  []
  (swap! counter inc))

(defmacro thread
  [& body]
  `(doto (Thread. (fn [] ~@body))
     (.start)))

(defmacro threadloop
  [bindings & body]
  `(thread (loop ~bindings ~@body)))

(def throttle? (atom false))

(defn start!
  []
  (Thread/sleep 1000)
  (if-let [t @throttle?]
    (threadloop []
      (Thread/sleep t)
      (>!! (:in *coordinator*) :heartbeat)
      (recur)))
  (>!! (:in *coordinator*) :start))

(defn- started?
  []
  (let [c (chan 0)
        s (do (>!! (:in *coordinator*) {:started? c})
              (<!! c))]
    (a/close! c)
    s))

;; TODO: check bounds
(defn set-throttle!
  [ms]
  (if (started?)
    (throw (Exception. "Can not enable throttling whilst already started"))
    (reset! throttle? ms)))

(defn register-source!
  [source]
  (>!! (:in *coordinator*) {:new-source (:in source)}))

(defmacro send-subscribers
  [subscribers changed? value id]
  `(doseq [sub# ~subscribers]
     (>! sub# {:changed? ~changed? :value ~value :from ~id})))

(defmacro send-subscribers*
  [subscribers changed? rdd collection id]
  `(doseq [sub# ~subscribers]
     (>! sub# {:changed? ~changed? :value ~rdd :collection ~collection :from ~id})))

(defmacro node-subscribe
  [source channel]
  `(>!! (:sub-chan ~source) {:subscribe ~channel}))

(defn subscribe-input
  [input]
  (let [c (chan)]
    (node-subscribe input c)
    c))

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

(defn subscribe-inputs
  [inputs]
  (map subscribe-input inputs))

(f/defsparkfn spark-identity [x] x)

(f/defsparkfn multiplicities-seq-fn
  [acc v]
  (merge-with + acc (assoc {} v 1)))

(f/defsparkfn multiplicities-com-fn
  [l r]
  (merge-with + l r))

(f/defsparkfn hash-to-multiset-map-fn
  [t]
  [(._1 t) (._2 t)])

(gen-class
  :name examples.FilterKeySizeFunction
  :implements [org.apache.spark.api.java.function.Function]
  :state state
  :init init
  :constructors {[Number] []}
  :prefix "filter-key-size-function-")

(defn filter-key-size-function-init
  [size]
  [[] size])

(defn filter-key-size-function-call
  [this val]
  (let [size (.state this)
        vals (._2 val)]
    (>= (count vals) size)))

;;;; PRIMITIVES ;;;;

(defrecord Coordinator [in])
(defrecord Node [id node-type return-type sub-chan])
(defrecord Source [id node-type return-type sub-chan in])   ;; isa Node
(defrecord Sink [id node-type])                             ;; isa Node

(defn make-coordinator
  []
  (let [in (chan)]
    (go-loop [msg (<! in)
              started? false
              sources []
              changes? false]
      (log/debug (str "coordinator received: " msg))
      (match msg
        {:new-source source-chan}
        (if started?
          (throw (Exception. "can not add new sources when already running"))
          (recur (<! in) started? (cons source-chan sources) changes?))

        {:destination id :value value}
        (do (when started?
              (doseq [source sources]
                (>! source msg)))
            (recur (<! in) started? sources true))

        {:started? reply-channel}
        (do (>! reply-channel started?)
            (recur (<! in) started? sources changes?))

        :heartbeat
        (do (when (and started? @throttle?)
              (doseq [source sources]
                (>! source :heartbeat)))
            (recur (<! in) started? sources false))

        :start (recur (<! in) true sources false)

        :stop (recur (<! in) false [] false)

        :reset (recur (<! in) false [] false)

        :else (recur (<! in) started? sources changes?)))
    (Coordinator. in)))

(def ^:dynamic ^:private *coordinator* (make-coordinator))

(defn parallelize-multiset
  [ms]
  (if (multiset-empty? ms)
    (emptyRDD)
    (let [ms (seq (to-multiset ms))]
      (f/parallelize sc ms))))

(defn parallelize-hash
  [hash]
  (if (hash-empty? hash)
    (emptyRDD)
    (let [hash (to-hash hash)]
      (->> (mapcat (fn [[k ms]]
                     (map ft/tuple (repeat k) ms))
                   hash)
           (f/parallelize-pairs sc)))))

(defn parallelize
  [c]
  (cond (multiset? c) (parallelize-multiset c)
        (hash? c) (parallelize-hash c)
        :else (throw (Exception. "type not supported"))))

(defn collect-multiset
  [rdd]
  (let [c (f/collect rdd)]
    (apply ms/multiset c)))

(defn collect-hash
  [rdd]
  (reduce (fn [hash tuple]
            (let [k (._1 tuple)
                  v (._2 tuple)]
              (update hash k #(if % (conj % v) (ms/multiset v)))))
          {}
          (f/collect rdd)))

(defn make-source-node
  [id [return-type & {:keys [timeout buffer] :or {timeout false buffer false}}] []]
  (let [in (chan)]
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
                          (if (= return-type :multiset) (make-multiset) (make-hash)))
              rdd (emptyRDD)
              changes? false]
      (log/debug (str "source " id " has received: " (seq msg)))
      (match msg
        {:subscribe subscriber}
        (recur (<! in) (cons subscriber subs) value rdd changes?)

        {:destination id :value new-value}
        (if @throttle?
          (let [new-coll (if (= return-type :multiset)
                           (multiset-insert* value new-value)
                           (hash-insert* value (first new-value) (second new-value)))]
            (recur (<! in) subs new-coll rdd true))
          (let [new-coll (if (= return-type :multiset)
                           (multiset-insert value new-value)
                           (hash-insert value (first new-value) (second new-value)))
                rdd (parallelize new-coll)]
            (send-subscribers* subs true rdd new-coll id)
            (recur (<! in) subs new-coll rdd false)))

        {:destination _}
        (do (when-not @throttle?
              (send-subscribers* subs false rdd value id))
            (recur (<! in) subs value rdd changes?))

        :heartbeat
        (if @throttle?
          (let [new-value (if (= return-type :multiset)
                            (multiset-copy value)
                            (hash-copy value))
                rdd (parallelize value)]
            (send-subscribers* subs changes? rdd value id)
            (recur (<! in) subs new-value rdd false))
          (recur (<! in) subs value rdd changes?))

        ;; TODO: error?
        :else (recur (<! in) subs value rdd changes?)))
    (let [source-node (Source. id ::source return-type in in)]
      (register-source! source-node)
      source-node)))

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

(defn make-union-node
  [id [] inputs]
  (let [sub-chan (chan)
        subscribers (atom [])
        inputs (subscribe-inputs inputs)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msgs (map <!! inputs)
              value (emptyRDD)]
      (log/debug (str "union node " id " has received: " msgs))
      (if (ormap :changed? msgs)
        (let [value (f/union (:value (first msgs))
                             (:value (second msgs)))]
          (send-subscribers @subscribers true value id)
          (recur (map <!! inputs) value))
        (do (send-subscribers @subscribers false value id)
            (recur (map <!! inputs) value))))
    (Node. id ::union :multiset sub-chan)))

(defn make-subtract-node
  [id [] inputs]
  (let [sub-chan (chan)
        subscribers (atom [])
        inputs (subscribe-inputs inputs)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msgs (map <!! inputs)
              value (emptyRDD)]
      (log/debug (str "subtract node " id " has received: " msgs))
      (if (ormap :changed? msgs)
        (let [value (f/subtract (:value (first msgs))
                                (:value (second msgs)))]
          (send-subscribers @subscribers true value id)
          (recur (map <!! inputs) value))
        (do (send-subscribers @subscribers false value id)
            (recur (map <!! inputs) value))))
    (Node. id ::subtract :multiset sub-chan)))

(defn make-intersection-node
  [id [] inputs]
  (let [sub-chan (chan)
        subscribers (atom [])
        inputs (subscribe-inputs inputs)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msgs (map <!! inputs)
              value (emptyRDD)]
      (log/debug (str "intersection node " id " has received: " msgs))
      (if (ormap :changed? msgs)
        (let [value (.intersection (:value (first msgs))
                                   (:value (second msgs)))]
          (send-subscribers @subscribers true value id)
          (recur (map <!! inputs) value))
        (do (send-subscribers @subscribers false value id)
            (recur (map <!! inputs) value))))
    (Node. id ::intersection :multiset sub-chan)))

(defn make-distinct-node
  [id [] [input]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (emptyRDD)]
      (log/debug (str "distinct node " id " has received: " msg))
      (if (:changed? msg)
        (let [value (f/distinct (:value msg))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id ::distinct :multiset sub-chan)))

(defn make-filter-key-size-node
  [id [size] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        filter-fn (examples.FilterKeySizeFunction. size)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (emptyRDD)]
      (log/debug (str "filter-key-size node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (-> (:value msg)
                        (f/group-by-key)
                        (.filter filter-fn)
                        (f/flat-map-values spark-identity))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id ::filter-key-size :hash sub-chan)))

;; TODO: initial
(defn make-reduce-by-key-node
  [id [fn initial] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (emptyRDD)]
      (log/debug (str "reduce-by-key node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (if (.isEmpty (:value msg))
                      (:value msg)
                      (-> (:value msg)
                          (f/reduce-by-key fn)))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id ::reduce-by-key :hash sub-chan)))

;; TODO: just use make-map-node
(defn make-hash-to-multiset-node
  [id [] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (emptyRDD)]
      (log/debug (str "hash-to-multiset node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (-> (:value msg)
                        (f/map hash-to-multiset-map-fn))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id ::hash-to-multiset :multiset sub-chan)))

;; TODO: Test if all works for empty RDDs
(defn make-map-node
  [id [f] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (emptyRDD)]
      (log/debug (str "map node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (-> (:value msg)
                        (f/map f))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id ::map :multiset sub-chan)))

;; TODO: Test if all works for empty RDDs
(defn make-map-by-key
  [id [f] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (emptyRDD)]
      (log/debug (str "map-by-key node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (-> (:value msg)
                        (f/map-values f))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id ::map-by-key :hash sub-chan)))

(defn make-multiplicities-node
  [id [] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (emptyRDD)]
      (log/debug (str "multiplicities node" id " has received: " msg))
      ;; NOTE: because we need to do a map and reduce, we use aggregate to combine the two
      (if (:changed? msg)
        (let [value (if (.isEmpty (:value msg))
                      (:value msg)
                      (-> (:value msg)
                          (f/aggregate {} multiplicities-seq-fn multiplicities-com-fn)
                          ((fn [m] (f/parallelize sc (vec m))))))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id ::multiplicities :multiset sub-chan)))

;; TODO: initial
(defn make-reduce-node
  [id [f initial] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (emptyRDD)]
      (log/debug (str "reduce node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (if (.isEmpty (:value msg))
                      (:value msg)
                      (-> (:value msg)
                          (f/reduce f)
                          ((fn [e] (f/parallelize sc [e])))))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id ::reduce :multiset sub-chan)))

(defn make-buffer-node
  [id [size] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        hash? (= (:return-type input-node) :hash)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              previous nil
              buffer (if hash? (make-buffered-hash size) (make-buffered-multiset size))
              rdd (emptyRDD)]
      (log/debug (str "buffer node " id " has received: " msg))
      (cond (and (:changed? msg) hash?)
            (let [removed (hash-removed (:collection msg))
                  inserted (hash-inserted (:collection msg))
                  buffer (hash-insert-and-remove buffer inserted removed)
                  rdd (parallelize-hash buffer)]
              (send-subscribers* @subscribers true rdd buffer id)
              (recur (<! input) (:collection msg) buffer rdd))

            (:changed? msg)
            (let [new (multiset-inserted (:collection msg))
                  removed (multiset-removed (:collection msg))
                  buffer (multiset-insert-and-remove buffer new removed)
                  rdd (parallelize-multiset buffer)]
              (send-subscribers* @subscribers true rdd buffer id)
              (recur (<! input) (:collection msg) buffer rdd))

            :else
            (do (send-subscribers* @subscribers false rdd buffer id)
                (recur (<! input) previous buffer rdd))))
    (Node. id ::buffer (:return-type input-node) sub-chan)))

(defn make-diff-add-node
  [id [] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        hash? (= (:return-type input-node) :hash)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              rdd (emptyRDD)]
      (log/debug (str "diff-add node " id " has received: " msg))
      (if (:changed? msg)
        (let [inserted ((if hash? hash-inserted multiset-inserted) (:collection msg))
              rdd (f/parallelize sc inserted)]
          (send-subscribers @subscribers true rdd id)
          (recur (<! input) rdd))
        (do (send-subscribers @subscribers false rdd id)
            (recur (<! input) rdd))))
    (Node. id ::diff-add :multiset sub-chan)))

;; NOTE: When using throttling, it is possible that some elements in the set of removed values
;; were never processed to begin with.
(defn make-diff-remove-node
  [id [] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        hash? (= (:return-type input-node) :hash)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              rdd (emptyRDD)]
      (log/debug (str "diff-remove node " id " has received: " msg))
      (if (:changed? msg)
        (let [inserted ((if hash? hash-removed multiset-removed) (:collection msg))
              rdd (f/parallelize sc inserted)]
          (send-subscribers @subscribers true rdd id)
          (recur (<! input) rdd))
        (do (send-subscribers @subscribers false rdd id)
            (recur (<! input) rdd))))
    (Node. id ::diff-remove :multiset sub-chan)))

(defn make-delay-node
  [id [] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              previous-c (if (= (:return-type input-node) :hash) (make-hash) (make-multiset))
              previous-r (emptyRDD)]
      (log/debug (str "delay node " id " has received: " msg))
      (send-subscribers* @subscribers (:changed? msg) previous-r previous-c id)
      (if (:changed? msg)
        (recur (<! input) (:collection msg) (:value msg))
        (recur (<! input) previous-c previous-r)))
    (Node. id ::delay (:return-type input-node) sub-chan)))

(gen-class
  :name examples.FilterFunction
  :implements [org.apache.spark.api.java.function.Function]
  :state state
  :init init
  :constructors {[Object] []}
  :prefix "filter-function-")

(defn filter-function-init
  [pred]
  [[] pred])

(defn filter-function-call ^Boolean
  [this val]
  (let [pred (.state this)]
    (pred val)))

(defn make-filter-node
  [id [pred] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        pred (examples.FilterFunction. pred)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (emptyRDD)]
      (log/debug (str "filter node " id " has received: " msg))
      (if (:changed? msg)
        (let [value (-> (:value msg)
                        (.filter pred))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id ::filter :multiset sub-chan)))

;; TODO: use serialisable function?
(defn make-filter-by-key-node
  [id [pred] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        filter-fn (fn/function (fn [^Tuple2 t] (pred (._2 t))))]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (emptyRDD)]
      (log/debug (str "filter-by-key node " id " has received: " msg))
      (if (:changed? msg)
        (let [value (-> (:value msg)
                        (.filter filter-fn))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id ::filter-by-key :hash sub-chan)))

(defn redis
  [host queue & {:keys [key buffer timeout] :or {key false buffer false timeout false}}]
  (let [id (new-id!)]
    (make-redis-node id [(if key :hash :multiset) host queue key buffer timeout] [])))

(defn print**
  [input-node form]
  (let [id (new-id!)
        input (subscribe-input input-node)
        collector (if (= (:return-type input-node) :hash)
                    collect-hash
                    collect-multiset)]
    (go-loop [msg (<! input)]
      (when (:changed? msg)
        (let [s (with-out-str
                  (-> (:value msg)
                      (collector)
                      (pprint)))]
          (locking print-lock
            (print (str form ": " s))
            (flush))))
      (recur (<! input)))))

(defmacro print*
  [input-form]
  `(print** ~input-form (quote ~input-form)))

(defn filter-key-size
  [input size]
  (make-filter-key-size-node (new-id!) [size] [input]))

;; TODO: initial
(defn reduce-by-key
  [input fn]
  (make-reduce-by-key-node (new-id!) [fn false] [input]))

(defn hash-to-multiset
  [input]
  (make-hash-to-multiset-node (new-id!) [] [input]))

(defn map*
  [input f]
  (make-map-node (new-id!) [f] [input]))

(defn map-by-key
  [input f]
  (make-map-by-key (new-id!) [f] [input]))

(defn multiplicities
  [input]
  (make-multiplicities-node (new-id!) [] [input]))

;; TODO: initial
(defn reduce*
  [input fn]
  (make-reduce-node (new-id!) [fn false] [input]))

(defn buffer
  [input size]
  (make-buffer-node (new-id!) [size] [input]))

(defn diff-add
  [input]
  (make-diff-add-node (new-id!) [] [input]))

(defn diff-remove
  [input]
  (make-diff-remove-node (new-id!) [] [input]))

(defn delay
  [input]
  (make-delay-node (new-id!) [] [input]))

(defn filter*
  [input pred]
  (make-filter-node (new-id!) [pred] [input]))

(defn filter-by-key
  [input pred]
  (make-filter-by-key-node (new-id!) [pred] [input]))

(defn union
  [left right]
  (let [id (new-id!)]
    (make-union-node id [] [left right])))

(defn subtract
  [left right]
  (let [id (new-id!)]
    (make-subtract-node id [] [left right])))

(defn distinct*
  [input]
  (let [id (new-id!)]
    (make-distinct-node id [] [input])))

;; TODO: correct behaviour for MULTISETS
(defn intersection
  [left right]
  (let [id (new-id!)]
    (make-intersection-node id [] [left right])))

(defn calculate-direction
  [[cur_lat cur_lon] [pre_lat pre_lon]]
  (let [y (* (Math/sin (- cur_lon pre_lon)) (Math/cos cur_lat))
        x (- (* (Math/cos pre_lat) (Math/sin cur_lat))
             (* (Math/sin pre_lat) (Math/cos cur_lat) (Math/cos (- cur_lon pre_lon))))
        bearing (Math/atan2 y x)
        deg (mod (+ (* bearing (/ 180.0 Math/PI)) 360) 360)]
    (cond (and (>= deg 315.) (<= deg 45.)) :east
          (and (>= deg 45.) (<= deg 135.)) :north
          (and (>= deg 135.) (<= deg 225.)) :west
          :else :south)))

(def redis-host "localhost")
;(def redis-host "134.184.49.17")
(def redis-key "bxlqueue")

;; TODO: (future work) type hints
;; TODO: (future work) wrap all functions in map and filter in a serialisable

(defn -main
  [& args]

  (setup-spark! {:app-name "sparky"
                 :master   "local[*]"})

  (comment)
  (let [r (redis "localhost" "q1")
        ;f1 (filter* r even?)
        ;f2 (filter* r odd?)
        f1 (filter* r #(= (mod % 5) 0))
        f2 (filter* r #(= (mod % 3) 0))
        ;f1 (filter* r (f/fn [n] (= (mod n 5) 0)))
        ;f2 (filter* r (f/fn [n] (= (mod n 3) 0)))
        u (union f1 f2)
        ;i (intersection f1 f2)
        ;s1 (subtract f1 f2)
        ;s2 (subtract f2 f1)
        d (distinct* u)
        ]
    (print* r)
    (print* f1)
    (print* f2)
    ;(print* u)
    ;(print* i)
    ;(print* s1)
    ;(print* s2)
    (print* d)

    ;(set-throttle! 1000)
    (start!)
    (println "Let's go"))

  (comment
    (let [l (redis "localhost" "q1" :buffer 2)
          r (redis "localhost" "q2")
          u (union l r)]
      (print* u)
      (start!)
      (println "Let's go!")))

  (comment
    #(let [t1 (ftime/parse (:time %1))
           t2 (ftime/parse (:time %2))]
      (if (time/before? t1 t2)
        (calculate-direction (:position %2) (:position %1))
        (calculate-direction (:position %1) (:position %2)))))

  (comment
    (let [r1 (redis redis-host redis-key :key :user-id :buffer 2)
          r2 (filter-key-size r1 2)
          r3 (reduce-by-key r2
                            (f/fn [l r]
                              (let [t1 (ftime/parse (:time l))
                                    t2 (ftime/parse (:time r))]
                                (if (time/before? t1 t2)
                                  (calculate-direction (:position r) (:position l))
                                  (calculate-direction (:position l) (:position r))))))
          r4 (hash-to-multiset r3)
          r5 (map* r4 (f/fn [t] (second t)))
          r6 (multiplicities r5)
          r7 (reduce* r6 (f/fn [t1 t2]
                           (let [[d1 c1] t1
                                 [d2 c2] t2]
                             (if (> c1 c2) t1 t2))))]
      (print* r7)
      (set-throttle! 1000)
      (start!)
      (println "Let's go!"))))