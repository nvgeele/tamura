(ns tamura.config)

(def ^{:private true} default-config
  {:runtime :clj
   :throttle false
   :spark {:app-name "tamura-app"
           :master "local[*]"
           :checkpoint-dir "/tmp/checkpoint"}})

(def config (atom default-config))

;; TODO: set-throttle operation with bound checking

(defn throttle?
  []
  (get @config :throttle (get default-config :throttle)))

(defn runtime
  []
  (get @config :runtime (get default-config :runtime)))

(defn spark-app-name
  []
  (get-in @config [:spark :app-name] (get-in default-config [:spark :app-name])))

(defn spark-master
  []
  (get-in @config [:spark :master] (get-in default-config [:spark :master])))

(defn spark-checkpoint-dir
  []
  (get-in @config [:spark :checkpoint-dir] (get-in default-config [:spark :checkpoint-dir])))