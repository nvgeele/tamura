(ns examples.flambo
  (:require [flambo.conf :as conf]
            [flambo.api :as f]
            [flambo.tuple :as ft]
            [flambo.streaming :as fs]
            [flambo.function :as fn]
            [clojure.pprint :refer [pprint]]

            [clojure.edn :as edn]
            [multiset.core :as ms]

            [clj-time.format :as ftime]
            [clj-time.core :as time]
            )
  (:import [examples RedisReceiver]
           (org.apache.spark.streaming StateSpec)))

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
  (let [receiver (.receiverStream ssc (RedisReceiver. host queue))]
    {:type        ::redis
     :return-type (if key :hash :multiset)
     :stream      (if key
                    (-> (fs/map-to-pair receiver (f/fn [str] (let [m (edn/read-string str)]
                                                               (ft/tuple (get m key) (dissoc m key)))))
                        (.mapWithState (StateSpec/function (if buffer
                                                             (examples.MapFun. buffer)
                                                             (examples.MapFun.))))
                        (.stateSnapshots))
                    (-> (fs/map-to-pair receiver #(ft/tuple 0 (edn/read-string %)))
                        (.mapWithState (StateSpec/function (if buffer
                                                             (examples.MapFun. buffer)
                                                             (examples.MapFun.))))
                        (.stateSnapshots)
                        (.flatMapToPair (examples.FlatMapMultisetSource.))))}))
(derive ::redis ::source)

(defn filter-key-size
  [input size]
  (assert*
    (= (:return-type input) :hash) "filter-key-size input must be a hash")
  {:type ::filter-key-size
   :return-type :hash
   :stream (.filter (:stream input) (examples.FilterKeySizeFunction. size))})

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

(defn multiplicities
  [input]
  (assert*
    (= (:return-type input) :multiset) "multiplicities input must be a multiset")
  {:type ::multiplicities
   :return-type :multiset
   :stream (-> (fs/map (:stream input) #(assoc {} % 1))
               (.reduce (fn/function2 #(merge-with + %1 %2)))
               (fs/flat-map vec))})

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

(defn -main
  [& args]
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
    (.awaitTermination ssc)))