(ns tamura.config)

(def ^{:private true} default-config
  {:runtime :clj
   :throttle false
   :spark {:app-name "tamura-app"
           :master "local[*]"}})

(def config (atom default-config))

(defn throttle?
  []
  (get @config :throttle (get default-config :throttle)))