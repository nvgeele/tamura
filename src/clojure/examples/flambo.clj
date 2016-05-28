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

(gen-class
  :name examples.MapFun
  :implements [org.apache.spark.api.java.function.Function3]
  ;:state "state"
  ;:init "init"
  ;:constructors {[] []}
  :prefix "func-map-")

(defn func-map-call
  [this id optionalArg state]
  (let [val (.get optionalArg)
        set (conj (if (.exists state) (.get state) []) val)]
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

(defn filter-key-size
  [pair-stream size]
  (.filter pair-stream (examples.FilterKeySizeFunction. 2)))

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

;; Passing an initial value to FlatMapFun is a trick to deal with the fact that reduce in Spark Streaming
;; does not take an initial value...
(defn reduce-by-key
  ([input-stream function]
   (-> (.flatMapToPair input-stream (examples.FlatMapFun.))
       (fs/reduce-by-key function)
       (.groupByKey)))
  ([input-stream function initial]
   (-> (.flatMapToPair input-stream (examples.FlatMapFun. initial))
       (fs/reduce-by-key function)
       (.groupByKey))))

(defn hash-to-multiset
  [input]
  (-> (.flatMapToPair input (examples.FlatMapFun.))
      (fs/map (f/fn [t] [(._1 t) (._2 t)]))))

(defn map*
  [input f]
  (fs/map input f))

(comment
  (-> (t/redis redis-host redis-key :key :user-id :buffer 2)
      (t/filter-key-size 2)
      (t/reduce-by-key #(let [t1 (f/parse (:time %1))
                              t2 (f/parse (:time %2))]
                         (if (time/before? t1 t2)
                           (calculate-direction (:position %2) (:position %1))
                           (calculate-direction (:position %1) (:position %2)))))
      (t/hash-to-multiset)
      (t/map second)
      (t/multiplicities)
      (t/reduce (fn [t1 t2]
                  (let [[d1 c1] t1
                        [d2 c2] t2]
                    (if (> c1 c2) t1 t2)))))
  "Wat er nodig is hiervoor:
    - Redis receiver
    - Buffered source: 2 bijhouden per key
    - Enkel keys doorlaten met 2 of meer values
    - Per key reducen
    - De tuples omvormen tot een multiset (PairDStream naar gewone DStream)
    - Mappen over de tuples
    - De multiplicities berekenen (MOEILIJK...)
    - Alles reducen
    - Printen

   Waar er over moet nagedacht worden:
    - Hoe gaan we om met configuratie?
    - We kunnen met macros code genereren tijdens compile-time, maar hoe worden argumenten geevalueerd?
    - En niet alleen argumenten, what about definitie van de graph zelf?
    - Hoe zit het met echte reactive computations/incremental shit?
  "

  )

(comment
  "{:user-id 1, :position [0.41497792244969556 0.49798402446719936], :time \"2016-05-28T14:48:51.331Z\"}"
  "{:user-id 2, :position [0.4113255447899179 0.4653541009535579], :time \"2016-05-28T14:48:51.383Z\"}"
  "{:user-id 3, :position [0.4113255447899179 0.4653541009535579], :time \"2016-05-28T14:48:51.383Z\"}"
  "{:user-id 1, :position [0.018471146703515684 0.07674546533119342], :time \"2016-05-28T14:49:51.387Z\"}"
  "{:user-id 2, :position [0.30995492659752755 0.7706770106817055], :time \"2016-05-28T14:49:51.328Z\"}"
  "{:user-id 3, :position [0.30995492659752755 0.7706770106817055], :time \"2016-05-28T14:49:51.328Z\"}")

(def sc (-> (conf/spark-conf)
            (conf/master "local[2]")
            (conf/app-name "flame_princess")
            (f/spark-context)))
(def ssc (fs/streaming-context sc 1000))

(fs/checkpoint ssc "/tmp/checkpoint")

(defn redis
  [host queue id]
  (-> (.receiverStream ssc (RedisReceiver. host queue))
      (fs/map-to-pair (f/fn [str] (let [m (edn/read-string str)]
                                    (ft/tuple (get m id) (dissoc m id)))))
      (.mapWithState (StateSpec/function (examples.MapFun.)))
      (.stateSnapshots)))

;; TODO: deal with singleton multisets!
(defn multiplicities
  [input-stream]
  (-> (fs/map input-stream #(assoc {} % 1))
      (.reduce (fn/function2 #(merge-with + %1 %2)))
      (fs/flat-map vec)))

;; TODO: initial value
(defn reduce*
  ([input-stream f]
    (.reduce input-stream (fn/function2 f)))
  ([input-stream f initial]
    (throw (Exception. "TODO"))))

;; TODO: buffering
(defn -main
  [& args]
  (let [complete (redis "localhost" "bxlqueue" :user-id)
        ;complete (redis "localhost" "kaka" :user-id)
        ;complete (redis "localhost" "kaka" :id)
        ;pairs (.flatMapToPair complete (examples.FlatMapFun.))
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
                                          (if (> c1 c2) t1 t2))))
        ]

    ;(.print complete)
    ;(.print filtered-on-size)
    ;(.print directions)
    ;(.print multiset)
    ;(.print directions*)
    (.print counts)
    (.print max-direction)


    (.start ssc)
    (.awaitTermination ssc)))