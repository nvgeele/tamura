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
  (:import [redis.clients.jedis Jedis]))

;;;; SET-UP & CONFIG ;;;;

(def local true)
(def config (-> (conf/spark-conf)
                (conf/app-name "sparky")))
(def sc (f/spark-context (if local (conf/master config "local[*]") config)))

(declare ^:dynamic ^:private *coordinator*)

;;;; HELPERS ;;;;

(def ^:private print-lock (Object.))

(defn println*
  [& args]
  #_(locking print-lock
    (apply println args))
  (apply println args))

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

(defn start!
  []
  (Thread/sleep 1000)
  (>!! (:in *coordinator*) :start))

(defn- started?
  []
  (let [c (chan 0)
        s (do (>!! (:in *coordinator*) {:started? c})
              (<!! c))]
    (a/close! c)
    s))

(defn register-source!
  [source]
  (>!! (:in *coordinator*) {:new-source (:in source)}))

(defmacro send-subscribers
  [subscribers changed? value id]
  `(doseq [sub# ~subscribers]
     (>! sub# {:changed? ~changed? :value ~value :from ~id})))

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

;;;; PRIMITIVES ;;;;

(defrecord Coordinator [in])
(defrecord Node [id node-type return-type sub-chan])
(defrecord Source [id node-type return-type sub-chan in]) ;; isa Node
(defrecord Sink [id node-type])                           ;; isa Node

;; TODO: like Spark Streaming, do micro-batching
(defn make-coordinator
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
              ;(println* "Coordinator forwarded something")
              (doseq [source sources]
                (>! source msg)))
            (recur (<! in) started? sources))

        {:started? reply-channel}
        (do (>! reply-channel started?)
            (recur (<! in) started? sources))

        :start (recur (<! in) true sources)

        :stop (recur (<! in) false [])

        :reset (recur (<! in) false [])

        :else (recur (<! in) started? sources)))
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

(defn collect-multiset
  [rdd]
  (let [c (f/collect rdd)]
    (apply ms/multiset c)))

(defn collect-hash
  [rdd]
  (let [c (reduce (fn [hash tuple]
                    (let [k (._1 tuple)
                          v (._2 tuple)]
                      (update hash k #(if % (conj % v) (ms/multiset v)))))
                  {}
                  (f/collect rdd))]
    c))

(defn make-source-node
  [id [return-type & {:keys [timeout buffer] :or {timeout false buffer false}}] []]
  (let [in (chan)
        transformer (if (= return-type :multiset)
                      parallelize-multiset
                      parallelize-hash)]
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
      ;(println* "Source" id "received something:" msg)
      (match msg
        {:subscribe subscriber}
        (recur (<! in) (cons subscriber subs) value)

        {:destination id :value new-value}
        (let [new-coll (if (= return-type :multiset)
                         (multiset-insert value new-value)
                         (hash-insert value (first new-value) (second new-value)))]
          ;(println* "Source" id "is propagating")
          (send-subscribers subs true (transformer new-coll) id)
          (recur (<! in) subs new-coll))

        ;; TODO: memoize transformer
        {:destination _}
        (do (send-subscribers subs false (transformer value) id)
            (recur (<! in) subs value))

        ;; TODO: error?
        :else (recur (<! in) subs value)))
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
        ;(println* "Redis received something")
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
      (log/debug (str "union-node " id " has received: " msgs))
      (if (ormap :changed? msgs)
        (let [value (f/union (:value (first msgs))
                             (:value (second msgs)))]
          (send-subscribers @subscribers true value id)
          (recur (map <!! inputs) value))
        (do (send-subscribers @subscribers false value id)
            (recur (map <!! inputs) value))))
    (Node. id ::union :multiset sub-chan)))

(defn make-filter-key-size-node
  [id [size] [input-node]]
  (let [sub-chan (chan)
        subscribers (atom [])
        input (subscribe-input input-node)]
    (subscriber-loop id sub-chan subscribers)
    (go-loop [msg (<! input)
              value (emptyRDD)]
      (log/debug (str "filter-key-size node" id " has received: " msg))
      (if (:changed? msg)
        (let [value (-> (:value msg)
                        (f/group-by-key)
                        (.filter (fn/function
                                   (fn [t]
                                     (let [vals (._2 t)]
                                       (>= (count vals) size)))))
                        (f/flat-map-values (fn [vals]
                                             vals)))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id ::filter-key-size :hash sub-chan)))

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
        (let [value (-> (:value msg)
                        (f/reduce-by-key fn))]
          (send-subscribers @subscribers true value id)
          (recur (<! input) value))
        (do (send-subscribers @subscribers false value id)
            (recur (<! input) value))))
    (Node. id ::reduce-by-key :hash sub-chan)))

(defn redis
  [host queue & {:keys [key buffer timeout] :or {key false buffer false timeout false}}]
  (let [id (new-id!)]
    (make-redis-node id [(if key :hash :multiset) host queue key buffer timeout] [])))

(defn union
  [left right]
  (let [id (new-id!)]
    (make-union-node id [] [left right])))

(defn print*
  [input-node]
  (let [id (new-id!)
        input (subscribe-input input-node)
        collector (if (= (:return-type input-node) :hash)
                    collect-hash
                    collect-multiset)]
    (go-loop [msg (<! input)]
      ;(log/error "print*" id "has received something")
      (when (:changed? msg)
        (-> (:value msg)
            (collector)
            (pprint)))
      (recur (<! input)))))

(defn filter-key-size
  [input size]
  (make-filter-key-size-node (new-id!) [size] [input]))

;; TODO: initial
(defn reduce-by-key
  [input fn]
  (make-reduce-by-key-node (new-id!) [fn false] [input]))

(defmacro hash-to-multiset [input] input)
(defmacro map* [input f] input)
(defmacro multiplicities [input] input)
(defmacro reduce* [input fn] input)

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

(comment
  lpush bxlqueue "{:user-id 1, :position [0.41497792244969556 0.49798402446719936], :time \"2016-05-28T14:48:51.331Z\"}" "{:user-id 2, :position [0.4113255447899179 0.4653541009535579], :time \"2016-05-28T14:48:51.383Z\"}" "{:user-id 3, :position [0.4113255447899179 0.4653541009535579], :time \"2016-05-28T14:48:51.383Z\"}"

  lpush bxlqueue "{:user-id 1, :position [0.018471146703515684 0.07674546533119342], :time \"2016-05-28T14:49:51.387Z\"}" "{:user-id 2, :position [0.30995492659752755 0.7706770106817055], :time \"2016-05-28T14:49:51.328Z\"}" "{:user-id 3, :position [0.30995492659752755 0.7706770106817055], :time \"2016-05-28T14:49:51.328Z\"}"
  )

(defn -main
  [& args]
  (comment
    (let [l (redis "localhost" "q1" :buffer 2)
          r (redis "localhost" "q2")
          u (union l r)]
      (print* u)
      (start!)
      (println "Let's go!")))

  (comment)
  (let [r1 (redis "localhost" "bxlqueue" :key :user-id :buffer 2)
        r2 (filter-key-size r1 2)
        r3 (reduce-by-key r2
                          #(let [t1 (ftime/parse (:time %1))
                                 t2 (ftime/parse (:time %2))]
                            (if (time/before? t1 t2)
                              (calculate-direction (:position %2) (:position %1))
                              (calculate-direction (:position %1) (:position %2)))))
        r4 (hash-to-multiset r3)
        r5 (map* r4 second)
        r6 (multiplicities r5)
        r7 (reduce* r6 (fn [t1 t2]
                         (let [[d1 c1] t1
                               [d2 c2] t2]
                           (if (> c1 c2) t1 t2))))]
    (print* r7)
    (start!)
    (println "Let's go!")))