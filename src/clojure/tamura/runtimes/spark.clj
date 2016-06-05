(ns tamura.runtimes.spark
  (:require [clojure.core.async :refer [>!! >! <!! <! go go-loop]]
            [clojure.core.match :refer [match]]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [clojure.pprint :refer [pprint]]
            [multiset.core :as ms]
            [tamura.config :as cfg]
            [tamura.node-types :as nt]
            [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [flambo.streaming :as fs]
            [flambo.function :as fn])
  (:use [tamura.coordinator]
        [tamura.datastructures]
        [tamura.node]
        [tamura.util])
  (:import [org.apache.spark.api.java JavaPairRDD JavaSparkContext]
           [redis.clients.jedis Jedis]
           [scala Tuple2])
  (:gen-class))

;; TODO: (future work) type hints
;; TODO: (future work) wrap all functions in map and filter in a serialisable
;; TODO: (:gen-class) everywhere

;;;; SET-UP & CONFIG ;;;;

(def ^{:private true} this-runtime :spark)

(declare ^:private ^JavaSparkContext sc)

(defn setup-spark!
  []
  (alter-var-root
    (var sc)
    (fn [& args]
      (-> (conf/spark-conf)
          (conf/app-name (cfg/spark-app-name))
          (conf/master (cfg/spark-master))
          (f/spark-context)))))

;;;; HELPERS ;;;;

(def ^:private print-lock (Object.))

(defn- emptyRDD
  []
  (.emptyRDD sc))

(defn parallelize-multiset*
  [ms]
  (if (empty? ms)
    (emptyRDD)
    (->> (ms/multiplicities ms)
         (map (fn [[el m]]
                (ft/tuple el m)))
         (f/parallelize-pairs sc))))

(defn parallelize-hash*
  [hash]
  (if (empty? hash)
    (emptyRDD)
    (->> (mapcat (fn [[k ms]]
                   (map ft/tuple (repeat k) ms))
                 hash)
         (f/parallelize-pairs sc))))

(defn parallelize-multiset
  [ms]
  (if (multiset-empty? ms)
    (emptyRDD)
    (parallelize-multiset* (to-multiset ms))))

(defn parallelize-hash
  [hash]
  (if (hash-empty? hash)
    (emptyRDD)
    (parallelize-hash* (to-hash hash))))

(defn parallelize
  [c]
  (cond (multiset? c) (parallelize-multiset c)
        (hash? c) (parallelize-hash c)
        :else (throw (Exception. "type not supported"))))

(defn collect-multiset*
  [^JavaPairRDD rdd]
  (->> (f/collect rdd)
       (reduce (fn [hash ^Tuple2 tuple]
                 (let [e (._1 tuple)
                       m (._2 tuple)]
                   (assoc hash e m)))
               {})
       (ms/multiplicities->multiset)))

(defn collect-hash*
  [rdd]
  (reduce (fn [hash ^Tuple2 tuple]
            (let [k (._1 tuple)
                  v (._2 tuple)]
              (update hash k #(if % (conj % v) (ms/multiset v)))))
          {}
          (f/collect rdd)))

(defn collect-multiset
  [ms]
  (make-multiset (collect-multiset* ms)))

(defn collect-hash
  [hash]
  (make-hash (collect-hash* hash)))

;;;; SPARK CLASSES AND FUNCTIONS ;;;;

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
  :name tamura.runtimes.FilterKeySizeFunction
  :implements [org.apache.spark.api.java.function.Function]
  :state state
  :init init
  :constructors {[Number] []}
  :prefix "filter-key-size-function-")

(defn filter-key-size-function-init
  [size]
  [[] size])

(defn filter-key-size-function-call
  [^tamura.runtimes.FilterKeySizeFunction this
   ^Tuple2 val]
  (let [size (.state this)
        vals (._2 val)]
    (>= (count vals) size)))

(gen-class
  :name tamura.runtimes.ReduceFunction
  :implements [org.apache.spark.api.java.function.Function2]
  :state state
  :init init
  :constructors {[Object] []}
  :prefix "reduce-function-")

(defn reduce-function-init
  [f]
  [[] f])

(defn reduce-function-call
  [^tamura.runtimes.ReduceFunction this lval rval]
  (let [f (.state this)]
    (f lval rval)))

(gen-class
  :name tamura.runtimes.FilterFunction
  :implements [org.apache.spark.api.java.function.Function]
  :state state
  :init init
  :constructors {[Object] []}
  :prefix "filter-function-")

(defn filter-function-init
  [pred]
  [[] pred])

(defn filter-function-call ^Boolean
[^tamura.runtimes.FilterFunction this val]
  (let [pred (.state this)]
    (pred val)))

(defn- rdd-multiplicities
  [rdd]
  (if (.isEmpty rdd)
    {}
    (f/aggregate rdd {} multiplicities-seq-fn multiplicities-com-fn)))

(gen-class
  :name tamura.runtimes.MultisetMapFunction
  :implements [org.apache.spark.api.java.function.PairFunction]
  :state state
  :init init
  :constructors {[Object] []}
  :prefix "multiset-map-function-")

(defn multiset-map-function-init
  [f]
  [[] f])

(defn multiset-map-function-call
  [^tamura.runtimes.MultisetMapFunction this
   ^Tuple2 tuple]
  (let [f (.state this)]
    (let [el (._1 tuple)
          m (._2 tuple)]
      (ft/tuple (f el) m))))

;;;; PRIMITIVES ;;;;

;;;;           SOURCES           ;;;;

;; TODO: capture the value of cfg/throttle? at construction?
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
        (if (cfg/throttle?)
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
        (do (when-not (cfg/throttle?)
              (send-subscribers* subs false rdd value id))
            (recur (<! in) subs value rdd changes?))

        :heartbeat
        (if (cfg/throttle?)
          (let [new-value (if (= return-type :multiset)
                            (multiset-copy value)
                            (hash-copy value))
                rdd (parallelize value)]
            (send-subscribers* subs changes? rdd value id)
            (recur (<! in) subs new-value rdd false))
          (recur (<! in) subs value rdd changes?))

        ;; TODO: error?
        :else (recur (<! in) subs value rdd changes?)))
    (make-source id nt/source return-type in in)))
(register-constructor! this-runtime nt/source make-source-node)

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
(register-constructor! this-runtime nt/redis make-redis-node)

;;;;  "POLYMORPHIC" OPERATIONS   ;;;;

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
    (make-node id nt/delay (:return-type input-node) sub-chan)))
(register-constructor! this-runtime nt/delay make-delay-node)

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
    (make-node id nt/buffer (:return-type input-node) sub-chan)))
(register-constructor! this-runtime nt/buffer make-buffer-node)

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
    (make-node id nt/diff-add :multiset sub-chan)))
(register-constructor! this-runtime nt/diff-add make-diff-add-node)

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
    (make-node id nt/diff-remove :multiset sub-chan)))
(register-constructor! this-runtime nt/diff-remove make-diff-remove-node)

(defn make-print-node
  [id [form] [input-node]]
  (let [input (subscribe-input input-node)
        collector (if (= (:return-type input-node) :hash)
                    collect-hash*
                    collect-multiset*)]
    (go-loop [msg (<! input)]
      (log/debug (str "print node " id " has received: " msg))
      (when (:changed? msg)
        (let [s (with-out-str (-> (:value msg)
                                  (collector)
                                  (pprint)))]
          (locking print-lock
            (print (str form ": " s))
            (flush))))
      (recur (<! input)))
    (make-sink id nt/print)))
(register-constructor! this-runtime nt/print make-print-node)

;;;;     MULTISET OPERATIONS     ;;;;

;; TODO: Test if all works for empty RDDs
;; TODO: Serializable function
(defn make-map-node
  [id [f] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        map-fn (tamura.runtimes.MultisetMapFunction. f)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (emptyRDD)]
      (log/debug (str "map node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (-> (:value msg)
                        (.mapToPair map-fn))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (make-node id nt/map :multiset sub-chan)))
(register-constructor! this-runtime nt/map make-map-node)

;; TODO: initial
;; TODO: wrap function
(defn make-reduce-node
  [id [f initial] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        reduce-fn (tamura.runtimes.ReduceFunction. f)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (emptyRDD)]
      (log/debug (str "reduce node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (if (.isEmpty (:value msg))
                      (:value msg)
                      (-> (:value msg)
                          (.reduce reduce-fn)
                          ((fn [e] (f/parallelize sc [e])))))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (make-node id nt/reduce :multiset sub-chan)))
(register-constructor! this-runtime nt/reduce make-reduce-node)

(defn make-filter-node
  [id [pred] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        pred (tamura.runtimes.FilterFunction. pred)]
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
    (make-node id nt/filter :multiset sub-chan)))
(register-constructor! this-runtime nt/filter make-filter-node)

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
        (let [m (rdd-multiplicities (:value msg))
              value (if (empty? m)
                      (:value msg)
                      (f/parallelize sc (vec m)))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (make-node id nt/multiplicities :multiset sub-chan)))
(register-constructor! this-runtime nt/multiplicities make-multiplicities-node)

;; TODO: can we make these operations 100% distributed?
;; https://en.wikipedia.org/wiki/Multiset#Multiplicity_function
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
        (let [lmul (rdd-multiplicities (:value (first msgs)))
              rmul (rdd-multiplicities (:value (second msgs)))
              value (-> (merge-with max lmul rmul)
                        (multiplicities->multiset)
                        (parallelize))]
          (send-subscribers @subscribers true value id)
          (recur (map <!! inputs) value))
        (do (send-subscribers @subscribers false value id)
            (recur (map <!! inputs) value))))
    (make-node id nt/union :multiset sub-chan)))
(register-constructor! this-runtime nt/union make-union-node)

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
        (let [lmul (rdd-multiplicities (:value (first msgs)))
              rmul (rdd-multiplicities (:value (second msgs)))
              value (-> (multiset-subtract (multiplicities->multiset lmul)
                                           (multiplicities->multiset rmul))
                        (parallelize))]
          (send-subscribers @subscribers true value id)
          (recur (map <!! inputs) value))
        (do (send-subscribers @subscribers false value id)
            (recur (map <!! inputs) value))))
    (make-node id nt/subtract :multiset sub-chan)))
(register-constructor! this-runtime nt/subtract make-subtract-node)

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
        (let [lmul (rdd-multiplicities (:value (first msgs)))
              rmul (rdd-multiplicities (:value (second msgs)))
              value (-> (multiset-intersection (multiplicities->multiset lmul)
                                               (multiplicities->multiset rmul))
                        (parallelize))]
          (send-subscribers @subscribers true value id)
          (recur (map <!! inputs) value))
        (do (send-subscribers @subscribers false value id)
            (recur (map <!! inputs) value))))
    (make-node id nt/intersection :multiset sub-chan)))
(register-constructor! this-runtime nt/intersection make-intersection-node)

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
    (make-node id nt/distinct :multiset sub-chan)))
(register-constructor! this-runtime nt/distinct make-distinct-node)

;;;;       HASH OPERATIONS       ;;;;

;; TODO: Test if all works for empty RDDs
;; TODO: Serializable function
(defn make-map-by-key-node
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
    (make-node id nt/map-by-key :hash sub-chan)))
(register-constructor! this-runtime nt/map-by-key make-map-by-key-node)

;; TODO: initial
;; TODO: wrap function
(defn make-reduce-by-key-node
  [id [fn initial] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        reduce-fn (tamura.runtimes.ReduceFunction. fn)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (emptyRDD)]
      (log/debug (str "reduce-by-key node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (if (.isEmpty (:value msg))
                      (:value msg)
                      (-> (:value msg)
                          (.reduceByKey reduce-fn)))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (make-node id nt/reduce-by-key :hash sub-chan)))
(register-constructor! this-runtime nt/reduce-by-key make-reduce-by-key-node)

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
    (make-node id nt/filter-by-key :hash sub-chan)))
(register-constructor! this-runtime nt/filter-by-key make-filter-by-key-node)

(defn make-filter-key-size-node
  [id [size] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)
        filter-fn (tamura.runtimes.FilterKeySizeFunction. size)]
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
    (make-node id nt/filter-key-size :hash sub-chan)))
(register-constructor! this-runtime nt/filter-key-size make-filter-key-size-node)

;; TODO: just use make-map-node?
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
    (make-node id nt/hash-to-multiset :multiset sub-chan)))
(register-constructor! this-runtime nt/hash-to-multiset make-hash-to-multiset-node)