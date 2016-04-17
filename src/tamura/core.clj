(ns tamura.core
  (:require [clojure.core :as core]
            [potemkin :as p]
            [tamura.macros :as macros]
            [tamura.values :as values]
            [tamura.funcs :as funcs])
  (:import [redis.clients.jedis JedisPool]))

(p/import-vars
  [tamura.macros

   def
   defn
   defsig]

  [tamura.funcs

   ;; map

   ])

(defmacro install
  "Installs"
  []
  ;; Check if :lang :tamura in metadata?
  ;; Overwrite eval?
  ;; Overwrite macros?

  (println "Installing tamura...")
  nil)

;; TODO: update height
(defprotocol Producer
  (value [this])
  (subscribe [this reactor])
  (height [this])

  (tick [this]))

(defprotocol Reactor
  (value [this])
  (subscribe [this reactor])
  (height [this])

  (update [this tick value]))

;; TODO: use threadpool?
(core/defn- make-producer
  [type rate args]
  (let [obj (eval `(new ~type ~@args))
        t (Thread.
            (fn [] (loop
                     (tick obj)
                     (Thread/sleep rate)
                     (recur))))]
    (.start t)
    obj))

(core/defn update-subscribers
  [subscribers tick value]
  (doseq [sub subscribers]
    (update sub tick value)))

;; f *must* produce a new value
(deftype FunctionProducer
  [f state]
  Reactor
  (value [this]
    (:value @state))
  (subscribe [this reactor]
    (swap! state #(assoc % :subscribers (cons reactor (:subscribers %)))))
  (height [this]
    (:height @state))
  (update [this tick value]
    (let [v (f tick value)]
      (swap! state assoc :value v)
      (update-subscribers (:subscribers @state) nil (:value @state)))))

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
              (swap! state assoc :value v)
              (update-subscribers (:subscribers @state) nil v)))]
    (v/make-eventstream (make-producer RedisSource 10 [f (atom state)]))))

(core/def redis make-redis)

#_(defn map
      [f lst]
      (if (isa? lst v/Signal)
        (throw (Exception. "ToDo"))
        (core/map f lst)))

#_(defn lift
      [f]
      (fn [arg]
        (f arg)))

;; (core/defn make-printer)

(core/defn -main
  [& args]
  (let [pool (JedisPool. "localhost")
        conn (.getResource pool)]
    (loop []
      (if-let [v (.rpop conn "bxlqueue")]
        (println v))
      (Thread/sleep 100)
      (recur)))
  #_(when (not (= (count args) 1))
      (println "Provide a source file please")
      (System/exit 1))
  ;; (println "Go:") (flush)
  #_(let [r (read)
          form `(do (ns tamura.read-code
                      (:require [tamura.core :refer :all]
                                [tamura.macros :refer :all]))
                    ~r)]
      ;; (println (macroexpand form))
      (eval r))

  )