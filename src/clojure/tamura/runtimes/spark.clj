(ns tamura.runtimes.spark
  (:require [clojure.core.async :refer [>!! >! <!! <! go go-loop]]
            [clojure.core.match :refer [match]]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]
            [clojure.pprint :refer [pprint]]
            [multiset.core :as ms]
            [tamura.config :as cfg]
            [tamura.node-types :as nt]
    ;; TODO: remove profiling
            [tamura.profile :as profile]
            [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [flambo.streaming :as fs]
            [flambo.function :as fn])
  (:use [tamura.coordinator]
        [tamura.datastructures]
        [tamura.node]
        [tamura.util])
  (:import [com.google.common.base Optional]
           [org.apache.spark.api.java JavaPairRDD JavaSparkContext]
           [org.apache.spark.streaming.api.java JavaStreamingContext]
           [redis.clients.jedis Jedis]
           [scala Tuple2]
           [tamura.spark.receivers RedisReceiver]
           [org.apache.spark.streaming Durations])
  (:gen-class))

;; TODO: (future work) type hints
;; TODO: (future work) wrap all functions in map and filter in a serialisable
;; TODO: (:gen-class) everywhere

;;;; SET-UP & CONFIG ;;;;

(def this-runtime :spark)

(def ^JavaSparkContext sc nil)
(def ^JavaStreamingContext ssc nil)

(defn- setup-streaming-context!
  []
  (alter-var-root
    (var ssc)
    (fn [& args]
      (JavaStreamingContext. sc (Durations/milliseconds (cfg/throttle?))))))

(defn setup-spark!
  []
  (when-not sc
    (alter-var-root
      (var sc)
      (fn [& args]
        (-> (conf/spark-conf)
            (conf/app-name (cfg/spark-app-name))
            (conf/master (cfg/spark-master))
            (f/spark-context)))))
  (when (cfg/throttle?)
    (setup-streaming-context!)))

(defn start-spark!
  []
  (when (cfg/throttle?)
    ;; TODO: do we really need a checkpoint dir?
    (.checkpoint ssc (cfg/spark-checkpoint-dir))
    (.start ssc)))

(defn stop-spark!
  []
  (when (cfg/throttle?)
    (.stop ssc false)))

;;;; HELPERS ;;;;

(def ^:private print-lock (Object.))

(defn- empty-rdd
  []
  (.emptyRDD sc))

(defn- empty-pair-rdd
  []
  (JavaPairRDD/fromJavaRDD (.emptyRDD sc)))

(defn parallelize-multiset*
  [ms]
  (if (empty? ms)
    (empty-pair-rdd)
    (->> (ms/multiplicities ms)
         (map (fn [[el m]]
                (ft/tuple el m)))
         (f/parallelize-pairs sc))))

(defn parallelize-hash*
  [hash]
  (if (empty? hash)
    (empty-pair-rdd)
    (->> (mapcat (fn [[k ms]]
                   (map ft/tuple (repeat k) ms))
                 hash)
         (f/parallelize-pairs sc))))

(defn parallelize-multiset
  [ms]
  (if (multiset-empty? ms)
    (empty-pair-rdd)
    (parallelize-multiset* (to-multiset ms))))

(defn parallelize-hash
  [hash]
  (if (hash-empty? hash)
    (empty-pair-rdd)
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
  ;(println (f/collect rdd))
  (reduce (fn [hash ^Tuple2 tuple]
            ;(println tuple)
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

(defn collect*
  [type rdd]
  (case type
    :multiset (collect-multiset* rdd)
    :hash (collect-hash* rdd)
    :else (throw (Exception. "collect*: unsupported type"))))

(defn collect
  [type rdd]
  (case type
    :multiset (collect-multiset rdd)
    :hash (collect-hash rdd)
    :else (throw (Exception. "collect: unsupported type"))))

;;;; TEMPORARY ;;;;
;; TODO: Remove

(def parallelize-times (agent 0))
(def collect-times (agent 0))

(defn parallelize-profile-fn
  [time]
  (send parallelize-times + time))

(defn collect-profile-fn
  [time]
  (send collect-times + time))

(profile/profile parallelize parallelize-profile-fn)

(profile/profile collect-hash* collect-profile-fn)
(profile/profile collect-multiset* collect-profile-fn)

;;;; SPARK CLASSES AND FUNCTIONS ;;;;

(f/defsparkfn spark-identity [x] x)

(f/defsparkfn spark-plus-fn [l r] (+ l r))

(f/defsparkfn multiplicities-fn
  [^Tuple2 tup]
  (let [e (._1 tup)
        m (._2 tup)]
    (ft/tuple [e m] 1)))

;; TODO: better type hints, or casting?
(f/defsparkfn multiset-union-map-fn
  [^Tuple2 tup]
  (let [el (._1 tup)
        left-m (._1 (._2 tup))
        right-m (._2 (._2 tup))]
    (ft/tuple el (max (.or left-m 0)
                      (.or right-m 0)))))

(f/defsparkfn multiset-subtract-map-fn
  [^Tuple2 tup]
  (let [el (._1 tup)
        left-m (._1 (._2 tup))
        right-m (._2 (._2 tup))]
    (ft/tuple el (max 0 (- left-m (.or right-m 0))))))

(f/defsparkfn multiset-intersection-map-fn
  [^Tuple2 tup]
  (let [el (._1 tup)
        left-m (._1 (._2 tup))
        right-m (._2 (._2 tup))]
    (ft/tuple el (min left-m (.or right-m 0)))))

(f/defsparkfn multiset-filter-empties-fn
  ^Boolean
  [^Tuple2 tup]
  (> (._2 tup) 0))

(f/defsparkfn multiset-distinct-map-fn
  [v]
  1)

(f/defsparkfn hash-to-multiset-map-fn
  [^Tuple2 grouped]
  (let [el (._1 grouped)
        ls (._2 grouped)]
    (ft/tuple [(._1 el) (._2 el)] (count ls))))

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
  :name tamura.runtimes.MultisetFilterFunction
  :implements [org.apache.spark.api.java.function.Function]
  :state state
  :init init
  :constructors {[Object] []}
  :prefix "multiset-filter-function-")

(defn multiset-filter-function-init
  [pred]
  [[] pred])

(defn multiset-filter-function-call
  ^Boolean
  [^tamura.runtimes.MultisetFilterFunction this
   ^Tuple2 val]
  (let [pred (.state this)
        el (._1 val)]
    (pred el)))

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
  ^Boolean
  [^tamura.runtimes.FilterKeySizeFunction this
   ^Tuple2 val]
  (let [size (.state this)
        vals (._2 val)]
    (>= (count vals) size)))

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

(gen-class
  :name tamura.runtimes.MultisetReduceFunction
  :implements [org.apache.spark.api.java.function.Function2]
  :state state
  :init init
  :constructors {[Object] []}
  :prefix "multiset-reduce-function-")

(defn multiset-reduce-function-init
  [f]
  [[] f])

;; NOTE: If there are a small amount of elements but with high multiplicities, it might be worth it
;; to first expand the PairRDD to a regular RDD and reduce this...
(defn multiset-reduce-function-call
  [^tamura.runtimes.MultisetReduceFunction this
   ^Tuple2 lval
   ^Tuple2 rval]
  (let [f (.state this)
        left-e (._1 lval)
        left-m (._2 lval)
        right-e (._1 rval)
        right-m (._2 rval)
        left-a (reduce f (repeat left-m left-e))
        right-a (reduce f (repeat right-m right-e))]
    (ft/tuple (f left-a right-a) 1)))

(defn- multiset-reduce-rdd
  [^tamura.runtimes.MultisetReduceFunction reduce-fn
   ^JavaPairRDD rdd]
  (if (.isEmpty rdd)
    rdd
    (-> (.reduce rdd reduce-fn)
        ((fn [e] (f/parallelize sc [e]))))))

(defn- multiset-filter-rdd
  [^tamura.runtimes.MultisetFilterFunction filter-fn
   ^JavaPairRDD rdd]
  (.filter rdd filter-fn))

(defn- multiset-multiplicities-rdd
  [^JavaPairRDD rdd]
  (f/map-to-pair rdd multiplicities-fn))

(defn- multiset-union-rdd
  [^JavaPairRDD left
   ^JavaPairRDD right]
  (-> (.fullOuterJoin left right)
      (f/map-to-pair multiset-union-map-fn)))

(defn- multiset-subtract-rdd
  [^JavaPairRDD left
   ^JavaPairRDD right]
  (-> (.leftOuterJoin left right)
      (f/map-to-pair multiset-subtract-map-fn)
      (f/filter multiset-filter-empties-fn)))

(defn- multiset-intersection-rdd
  [^JavaPairRDD left
   ^JavaPairRDD right]
  (-> (.leftOuterJoin left right)
      (f/map-to-pair multiset-intersection-map-fn)
      (f/filter multiset-filter-empties-fn)))

(defn- multiset-distinct-rdd
  [^JavaPairRDD rdd]
  (f/map-values rdd multiset-distinct-map-fn))

(defn- hash-to-multiset-rdd
  [^JavaPairRDD rdd]
  (-> (f/group-by rdd spark-identity)
      (f/map-to-pair hash-to-multiset-map-fn)))

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
              rdd (empty-pair-rdd)
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

        ;; TODO: implement
        ;; NOTE: if a source receives this message, we assume throttle? is not false
        {:destination id :values values}
        (let [new-coll (if (= return-type :multiset)
                         (reduce (fn [s v] (multiset-insert* s v)) value values)
                         (reduce (fn [h [k v]] (hash-insert* h k v)) value values))]
          (recur (<! in) subs new-coll rdd true))

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

;; TODO: if throttle is on, then do the following
;; - make a redis receiver for spark streaming
;; - foreachRDD collect, and push all
(defn make-redis-node
  [id [return-type host queue key buffer timeout] []]
  (if (cfg/throttle?)
    (let [source-node (make-source-node id [return-type :timeout timeout :buffer buffer] [])
          redis-input (.receiverStream ssc (RedisReceiver. host queue))]
      ;; TODO: helper class
      ;; TODO: bulk receive for coordinator
      (fs/foreach-rdd
        redis-input
        (fn [rdd time]
          (let [vals (map #(let [parsed (edn/read-string %)]
                            (if key
                              [(get parsed key) (dissoc parsed key)]
                              parsed))
                          (f/collect rdd))]
            (>!! (:in *coordinator*) {:destination id :values vals}))))
      source-node)
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
      source-node)))
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
              previous-r (empty-pair-rdd)]
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
              rdd (empty-pair-rdd)]
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
              rdd (empty-pair-rdd)]
      (log/debug (str "diff-add node " id " has received: " msg))
      (if (:changed? msg)
        (let [inserted ((if hash? hash-inserted multiset-inserted) (:collection msg))
              rdd (if (empty? inserted)
                    (empty-pair-rdd)
                    (parallelize-multiset* (apply ms/multiset inserted)))]
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
              rdd (empty-pair-rdd)]
      (log/debug (str "diff-remove node " id " has received: " msg))
      (if (:changed? msg)
        (let [removed ((if hash? hash-removed multiset-removed) (:collection msg))
              rdd (if (empty? removed)
                    (empty-pair-rdd)
                    (parallelize-multiset* (apply ms/multiset removed)))]
          (send-subscribers @subscribers true rdd id)
          (recur (<! input) rdd))
        (do (send-subscribers @subscribers false rdd id)
            (recur (<! input) rdd))))
    (make-node id nt/diff-remove :multiset sub-chan)))
(register-constructor! this-runtime nt/diff-remove make-diff-remove-node)

;; TODO: tests
(defn make-do-apply-node
  [id [action] input-nodes]
  (let [inputs (subscribe-inputs input-nodes)
        input-types (map :return-type input-nodes)]
    (go-loop [msgs (map <!! inputs)]
      (log/debug (str "do-apply node " id " has received: " (seq msgs)))
      (when (ormap :changed? msgs)
        (let [colls (map #(collect* %1 (:value %2)) input-types msgs)]
          (apply action colls)))
      (recur (map <!! inputs)))
    (make-sink id nt/do-apply)))
(register-constructor! this-runtime nt/do-apply make-do-apply-node)

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

(defn make-redis-out-node
  [id [host key flatten?] [input-node]]
  (let [input (subscribe-input input-node)
        conn (Jedis. host)
        hash? (= (:return-type input-node) :hash)
        collector (if (= (:return-type input-node) :hash)
                    collect-hash*
                    collect-multiset*)]
    (go-loop [msg (<! input)]
      (log/debug (str "redis-out node " id " has received: " msg))
      (when (:changed? msg)
        (let [data (collector (:value msg))
              array (if flatten?
                      (if hash?
                        (for [[k ms] data
                              v ms]
                          (pr-str [k v]))
                        (for [v data] (pr-str v)))
                      [(pr-str data)])]
          (.rpush conn key (into-array String array))))
      (recur (<! input)))
    (make-sink id nt/redis-out)))
(register-constructor! this-runtime nt/redis-out make-redis-out-node)

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
              value (empty-pair-rdd)]
      (log/debug (str "map node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (-> (:value msg)
                        (.mapToPair map-fn)
                        (f/reduce-by-key spark-plus-fn))]
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
        reduce-fn (tamura.runtimes.MultisetReduceFunction. f)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (empty-pair-rdd)]
      (log/debug (str "reduce node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (multiset-reduce-rdd reduce-fn (:value msg))]
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
        filter-fn (tamura.runtimes.MultisetFilterFunction. pred)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (empty-pair-rdd)]
      (log/debug (str "filter node " id " has received: " msg))
      (if (:changed? msg)
        (let [value (multiset-filter-rdd filter-fn (:value msg))]
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
              value (empty-pair-rdd)]
      (log/debug (str "multiplicities node" id " has received: " msg))
      ;; NOTE: because we need to do a map and reduce, we use aggregate to combine the two
      (if (:changed? msg)
        (let [value (multiset-multiplicities-rdd (:value msg))]
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
              value (empty-pair-rdd)]
      (log/debug (str "union node " id " has received: " msgs))
      (if (ormap :changed? msgs)
        (let [value (multiset-union-rdd (:value (first msgs))
                                        (:value (second msgs)))]
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
              value (empty-pair-rdd)]
      (log/debug (str "subtract node " id " has received: " msgs))
      (if (ormap :changed? msgs)
        (let [value (multiset-subtract-rdd (:value (first msgs))
                                           (:value (second msgs)))]
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
              value (empty-pair-rdd)]
      (log/debug (str "intersection node " id " has received: " msgs))
      (if (ormap :changed? msgs)
        (let [value (multiset-intersection-rdd (:value (first msgs))
                                               (:value (second msgs)))]
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
              value (empty-pair-rdd)]
      (log/debug (str "distinct node " id " has received: " msg))
      (if (:changed? msg)
        (let [value (multiset-distinct-rdd (:value msg))]
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
              value (empty-pair-rdd)]
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
              value (empty-pair-rdd)]
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
              value (empty-pair-rdd)]
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
              value (empty-pair-rdd)]
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
              value (empty-pair-rdd)]
      (log/debug (str "hash-to-multiset node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (hash-to-multiset-rdd (:value msg))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (make-node id nt/hash-to-multiset :multiset sub-chan)))
(register-constructor! this-runtime nt/hash-to-multiset make-hash-to-multiset-node)