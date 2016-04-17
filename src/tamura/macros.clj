(ns tamura.macros
  (:require [clojure.core :as core]
            [clojure.tools.logging :as log]
            [tamura.values :as v]))

;; TODO: letsig ?

(defmacro def
  [name value]
  (log/info "Yay, def!")
  `(def ~name (let [v# ~value]
                (if (v/signal? v#)
                  (throw (Exception. "Can not assign signals using `def'!"))
                  v#))))

(defmacro defsig
  [name value]
  (log/info "Yay, defsig!")
  `(def ~name (let [v# ~value]
                (if (v/signal? v#)
                  v#
                  (throw (Exception. "Can only assign signals using `defsig'!"))))))

(defmacro defn
  [name & args]
  (log/info "Yay, defn!")
  `(tamura.macros/def ~name (fn ~@args)))