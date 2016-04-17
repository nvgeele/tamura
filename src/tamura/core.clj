(ns tamura.core
  (:require [clojure.core :as core]
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

;; f *must* produce a new value
(deftype FunctionReactor
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

;; f should receive boolean, not the value, as the value produced could be false.
;; therefore f is responsible for swapping the new value in etc.
(deftype FunctionProducer
  [f state]
  Producer
  (value [this]
    (:value @state))
  (subscribe [this reactor]
    (swap! state #(assoc % :subscribers (cons reactor (:subscribers %)))))
  (height [this]
    (:height @state))
  (tick [this]
    (when (f state)
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
              (println "Received something")
              (swap! state assoc :value v)
              (update-subscribers (:subscribers @state) nil v)))
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