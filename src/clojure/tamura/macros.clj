(ns tamura.macros
  (:refer-clojure :exclude [defn])
  (:require [clojure.core :as core]
            [clojure.tools.logging :as log]
            [tamura.values :as v]))

(defmacro def
  [name value]
  (log/debug "Tamura def macro used")
  `(def ~name (let [v# ~value]
                (if (v/signal? v#)
                  (throw (Exception. "Can not assign signals using `def'!"))
                  v#))))

;; TODO: letsig ?
;; TODO: Maybe do not expand with def, but register node in an environment?
(defmacro defsig
  [name value]
  (log/debug "Tamura defsig macro used")
  `(def ~name (let [v# ~value]
                (if (v/signal? v#)
                  v#
                  (throw (Exception. "Can only assign signals using `defsig'!"))))))

(defmacro defn
  [name & args]
  (log/debug "Tamura defn macro used")
  `(tamura.macros/def ~name (fn ~@args)))