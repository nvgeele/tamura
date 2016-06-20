(ns tamura.config)

(def ^{:private true} default-config
  {:runtime :clj
   :throttle false
   :spark {:app-name "tamura-app"
           :master "local[*]"
           :checkpoint-dir "/tmp/checkpoint"
           :enable-stream-receivers false}})

(def config (atom default-config))

(defn reset-config!
  []
  (reset! config default-config))

;; TODO: set-throttle operation with bound checking
;; TODO: automatically generate getters and setters from default-config structure

(defn throttle?
  []
  (get @config :throttle (get default-config :throttle)))

(defn set-throttle!
  [throttle]
  (swap! config assoc :throttle throttle))

(defn runtime
  []
  (get @config :runtime (get default-config :runtime)))

(defn set-runtime!
  [runtime]
  (swap! config assoc :runtime runtime))

(defn spark-app-name
  []
  (get-in @config [:spark :app-name] (get-in default-config [:spark :app-name])))

(defn spark-master
  []
  (get-in @config [:spark :master] (get-in default-config [:spark :master])))

(defn set-spark-master!
  [spark-master]
  (swap! config assoc-in [:spark :master] spark-master))

(defn spark-checkpoint-dir
  []
  (get-in @config [:spark :checkpoint-dir] (get-in default-config [:spark :checkpoint-dir])))

(defn set-spark-checkpoint-dir!
  [checkpoint-dir]
  (swap! config assoc-in [:spark :checkpoint-dir] checkpoint-dir))

(defn spark-enable-stream-receivers
  []
  (get-in @config [:spark :enable-stream-receivers] (get-in default-config [:spark :enable-stream-receivers])))

(defn set-spark-enable-stream-receivers!
  [b]
  (swap! config assoc-in [:spark :enable-stream-receivers] (true? b)))