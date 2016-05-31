(ns ^{:skip-aot true} examples.flambo
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [flambo.streaming :as fs]
            [flambo.function :as fn]
            [clojure.pprint :refer [pprint]]

            [clojure.edn :as edn]
            [multiset.core :as ms]

            [clj-time.format :as ftime]
            [clj-time.core :as time])
  (:import [examples RedisReceiver]
           [org.apache.spark.streaming StateSpec]
           [org.apache.spark.api.java JavaPairRDD]
           [java.util LinkedList]))

;; SPARK (STREAMING) SETUP ;;

(def sc (-> (conf/spark-conf)
            (conf/master "local[*]")
            (conf/app-name "flame_princess")
            (f/spark-context)))
(def ssc (fs/streaming-context sc 1000))
(fs/checkpoint ssc "/tmp/checkpoint")

;; HELPERS ;;

(defmacro ^{:private true} assert-args
  [& pairs]
  `(do (when-not ~(first pairs)
         (throw (IllegalArgumentException.
                  (str (first ~'&form) " requires " ~(second pairs) " in " ~'*ns* ":" (:line (meta ~'&form))))))
       ~(let [more (nnext pairs)]
          (when more
            (list* `assert-args more)))))

(defmacro assert*
  [& pairs]
  (assert-args
    (>= (count pairs) 2) "2 or more expressions in the body"
    (even? (count pairs)) "an even amount of expressions")
  `(do (assert ~(first pairs) ~(second pairs))
       ~(let [more (nnext pairs)]
          (when more
            (list* `assert* more)))))

;; HELPER CLASSES ;;

(gen-class
  :name examples.MapFun
  :implements [org.apache.spark.api.java.function.Function3]
  :state state
  :init init
  :constructors {[] []
                 [Number] []}
  :prefix "func-map-")

(defn func-map-init
  ([] [[] {:buffered? false}])
  ([size] [[] {:buffered? true :size size}]))

;; TODO: We do not need to return the existing but just the key with a nil value
(defn func-map-call
  [this id optionalArg state]
  (let [val (.get optionalArg)
        s (.state this)
        existing (if (.exists state) (.get state) [])
        set (conj (if (and (not (empty? existing))
                           (boolean (:buffered? s))
                           (= (count existing) (:size s)))
                    (vec (rest existing))
                    existing)
                  val)]
    (.update state set)
    (ft/tuple id set)))

(gen-class
  :name examples.FlatMapFun
  :implements [org.apache.spark.api.java.function.PairFlatMapFunction]
  :prefix "func-flatmap-"
  :state state
  :init init
  :constructors {[Object] []
                 [] []})

(defn func-flatmap-init
  ([] [[] {:init? false}])
  ([val] [[] {:init? true :v val}]))

(defn func-flatmap-call
  [this val]
  (let [key (._1 val)
        vals (._2 val)
        mapped (map #(ft/tuple key %) vals)
        s (.state this)]
    (if (boolean (:init? s))
      (cons (ft/tuple key (:v s)) mapped)
      mapped)))

(gen-class
  :name examples.FlatMapMultisetSource
  :implements [org.apache.spark.api.java.function.PairFlatMapFunction]
  :prefix "flatmapms-")

(defn flatmapms-call
  [this val]
  (._2 val))

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

;; PRIMITIVES ;;

(defn redis
  [host queue & {:keys [key buffer timeout] :or {key false buffer false timeout false}}]
  (let [receiver (.receiverStream ssc (RedisReceiver. host queue))
        pairs (fs/map-to-pair receiver (if key
                                         (f/fn [str] (let [m (edn/read-string str)]
                                                       (ft/tuple (get m key) (dissoc m key))))
                                         #(ft/tuple 0 (edn/read-string %))))
        state (.mapWithState pairs (StateSpec/function (if buffer (examples.MapFun. buffer) (examples.MapFun.))))]
    {:type        ::redis
     :return-type (if key :hash :multiset)
     :state       state
     :stream      (if key
                    (.stateSnapshots state)
                    (-> (.stateSnapshots state)
                        (.flatMapToPair (examples.FlatMapMultisetSource.))))}))
(derive ::redis ::source)

(gen-class
  :name examples.DelayMapMultiset
  :implements [org.apache.spark.api.java.function.Function3]
  :prefix "delay-map-ms-")

(defn delay-map-ms-call
  [this id optionalArg state]
  (let [previous (if (.exists state) (.get state) [])]
    (.update state (.get optionalArg))
    (ft/tuple id previous)))

;; first gather everything under keys

;; TODO: delay (only after a source?)
(defn delay
  [input]
  {:stream (-> (:stream input)
               (fs/map-to-pair (f/fn [val] (ft/tuple 0 val)))
               (.groupByKey)
               (.mapWithState (StateSpec/function (examples.DelayMapMultiset.)))
               (.flatMapToPair (examples.FlatMapMultisetSource.)))})

;; TODO: buffer (only after a source)
(defn buffer
  [input])

;; TODO: diff-add / diff-remove (only after source, buffer, and maybe delay)
(defn diff-add
  [input])

(defn diff-remove
  [input])

;; TODO: test
(defn filter*
  [input pred]
  (assert*
    (= (:return-type input) :multiset) "filter input must be a multiset")
  {:type ::filter
   :return-type :multiset
   :stream (.filter (:stream input) (fn/function pred))})

;; TODO: test
(defn filter-by-key
  [input pred]
  (assert*
    (= (:return-type input) :hash) "filter-by-key input must be a hash")
  {:type ::filter-by-key
   :return-type :hash
   :stream (-> (.flatMapToPair (:stream input) (examples.FlatMapFun.))
               (.filter (fn/function2 (fn [k v] (pred v))))
               (.groupByKey))})

(defn filter-key-size
  [input size]
  (assert*
    (= (:return-type input) :hash) "filter-key-size input must be a hash")
  {:type ::filter-key-size
   :return-type :hash
   :stream (.filter (:stream input) (examples.FilterKeySizeFunction. size))})

(defn reduce*
  ([input f]
   (assert*
     (= (:return-type input) :multiset) "reduce input must be a multiset")
   {:type ::reduce
    :return-type :multiset
    :stream (.reduce (:stream input) (fn/function2 f))})
  ([input f initial]
   (assert*
     (= (:return-type input) :multiset) "reduce input must be a multiset")
   (throw (Exception. "TODO"))))

;; Passing an initial value to FlatMapFun is a trick to deal with the fact that reduce in Spark Streaming
;; does not take an initial value...
(defn reduce-by-key
  ([input function]
   (assert*
     (= (:return-type input) :hash) "reduce-by-key input must be a hash")
   {:type ::reduce-by-key
    :return-type :hash
    :stream (-> (.flatMapToPair (:stream input) (examples.FlatMapFun.))
                (fs/reduce-by-key function)
                (.groupByKey))})
  ([input function initial]
   (assert*
     (= (:return-type input) :hash) "reduce-by-key input must be a hash")
   {:type ::reduce-by-key
    :return-type :hash
    :stream (-> (.flatMapToPair (:stream input) (examples.FlatMapFun. initial))
                (fs/reduce-by-key function)
                (.groupByKey))}))

(defn hash-to-multiset
  [input]
  (assert*
    (= (:return-type input) :hash) "hash-to-multiset input must be a hash")
  {:type ::hash-to-multiset
   :return-type :multiset
   :stream (-> (.flatMapToPair (:stream input) (examples.FlatMapFun.))
               (fs/map (f/fn [t] [(._1 t) (._2 t)])))})

(defn map*
  [input f]
  (assert*
    (= (:return-type input) :multiset) "map input must be a multiset")
  {:type ::map
   :return-type :multiset
   :stream (fs/map (:stream input) f)})

;; TODO: test
(defn map-by-key
  [input f]
  (assert*
    (= (:return-type input) :hash) "map-by-key input must be a hash")
  {:type ::map-by-key
   :return-type :hash
   :stream (-> (.flatMapToPair (:stream input) (examples.FlatMapFun.))
               (.mapValues (fn/function f))
               (.groupByKey))})

;; TODO: maybe countByValue->FlatMap is more performant
;; TODO: maybe make multiplicities return a hash and then countByValue is ok
(defn multiplicities
  [input]
  (assert*
    (= (:return-type input) :multiset) "multiplicities input must be a multiset")
  {:type ::multiplicities
   :return-type :multiset
   :stream (-> (fs/map (:stream input) #(assoc {} % 1))
               (.reduce (fn/function2 #(merge-with + %1 %2)))
               (fs/flat-map vec))})

(defn print*
  [input]
  (.print (:stream input)))

;; THE REST ;;

(comment
  "Waar er over moet nagedacht worden:
    - Hoe gaan we om met configuratie?
    - We kunnen met macros code genereren tijdens compile-time, maar hoe worden argumenten geevalueerd?
    - En niet alleen argumenten, what about definitie van de graph zelf?
    - Hoe zit het met echte reactive computations/incremental shit?")

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

;; TODO: so what if we use a state to actually memoize stuff?

(comment
  "If we execute the following snippet, and add two numbers to test-ms, we will see that
   `reducing!' is printed every tick. This means that Spark Streaming is not incremental.

   In Tamura, the runtime uses the `changed?' flag to avoid redundant computation. It would be
   amazing to implement a similar method in Spark Streaming.

   Ideally, when using the .stateSnapshots method on source states, we would like to trigger computation
   in downstream nodes only when data was added or removed. How we do this however, I do not know at
   the moment."

  (print* (reduce* (redis "localhost" "test-ms")
                   (f/fn [l r]
                     (println "reducing!")
                     (+ l r)))))

(comment
  "Right now a hash is a stream of <K, Iterable<V>>,
   might be more performant of just having a stream of <K, V> tuples.")

(defn print-methods
  [obj]
  (let [methods (sort #(compare (.getName %1) (.getName %2)) (.getMethods (type obj)))]
    (doseq [m methods]
      (println "Method Name: " (.getName m))
      (println "Return Type: " (.getReturnType m) "\n"))))

(defn process-streamed
  [rdd]
  (let [l (LinkedList.)]
    (.add l rdd)
    (-> (.queueStream ssc l)
        (fs/map inc)
        (fs/print))))

(defn -main
  [& args]
  (comment
    (let [input (redis "localhost" "test-ms")
          delayed (delay input)]
      (print* input)
      (print* delayed)))
  ;(print* (delay (redis "localhost" "test-ms-buf" :buffer 2)))

  ;(print* (delay (redis "localhost" "test-hash" :key :id)))
  ;(print* (delay (redis "localhost" "test-hash-buf" :key :id :buffer 2)))

  ;(.start ssc)
  ;(.awaitTermination ssc)

  (process-streamed (f/parallelize sc [1 2 3]))

  (.start ssc)

  (process-streamed (f/parallelize sc [4 5 6]))

  (.awaitTermination ssc)

  (comment
    (-> (f/parallelize sc [{:a 1} {:b 2}])
        ;(f/map vals)
        (f/collect)
        (println))

    (let [l [(f/parallelize sc [1 2 3])
             (f/parallelize sc [4 5 6])
             (f/parallelize sc [7 8 9])]]))

  ;; NOTE: RDDs preserve order

  (comment
    (let [input (redis "localhost" "test-hash" :key :id)]
      (.foreachRDD (:state input)
                   (fn/void-function
                     (fn [rdd]
                       ;(println (type rdd))
                       ;(print-methods rdd)

                       (println (.id rdd))
                       ;(println (.first rdd))
                       (println (.hashCode rdd))
                       (println (.name rdd))

                       ;(System/exit 0)
                       )))
      (.start ssc)
      (.awaitTermination ssc)))

  (comment
    (let [input (redis "localhost" "test-hash" :key :id)
          state (:stream input)
          input (fs/map-to-pair (:state input) identity)
          grouped (.cogroup input state)]
      (.print state)
      (.print (.transformToPair grouped
                                (fn/function
                                  (fn [rdd]
                                    (let [changed? (f/aggregate rdd false
                                                                (fn [acc v]
                                                                  (let [input-vals (._1 (._2 v))]
                                                                    (or (boolean acc)
                                                                        (not (boolean (.isEmpty input-vals))))))
                                                                (fn [l r]
                                                                  (or l r)))]
                                      (comment (println "changed?:" changed?))
                                      (if changed?
                                        (f/flat-map-to-pair rdd (fn [v]
                                                                  (let [key (._1 v)
                                                                        state-vals (._2 (._2 v))]
                                                                    (map ft/tuple (repeat key) state-vals))))
                                        (JavaPairRDD/fromJavaRDD (.emptyRDD sc))))))))

      (.start ssc)
      (.awaitTermination ssc)))

  (comment
    (let [complete (redis "localhost" "bxlqueue" :key :user-id :buffer 2)
          filtered-on-size (filter-key-size complete 2)
          directions (reduce-by-key filtered-on-size
                                    #(let [t1 (ftime/parse (:time %1))
                                           t2 (ftime/parse (:time %2))]
                                      (if (time/before? t1 t2)
                                        (calculate-direction (:position %2) (:position %1))
                                        (calculate-direction (:position %1) (:position %2)))))
          multiset (hash-to-multiset directions)
          directions* (map* multiset second)
          counts (multiplicities directions*)
          max-direction (reduce* counts (fn [t1 t2]
                                          (let [[d1 c1] t1
                                                [d2 c2] t2]
                                            (if (> c1 c2) t1 t2))))]
      (print* counts)
      (print* max-direction)

      (.start ssc)
      (.awaitTermination ssc))))