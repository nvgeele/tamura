(ns tamura.coordinator
  (:require [clojure.core.async :as a :refer [>!! >! <!! <! go go-loop]]
            [clojure.core.match :refer [match]]
            [clojure.tools.logging :as log]
            [tamura.config :as cfg])
  (:use [tamura.util]))

(defrecord Coordinator [in])

;; TODO: capture the value of cfg/throttle? at construction?
;; TODO: do not propagate heartbeat if no changes?
(defn make-coordinator
  []
  (let [in (chan)]
    (go-loop [msg (<! in)
              started? false
              sources []
              changes? false]
      (log/debug (str "coordinator received: " msg))
      (match msg
        {:new-source source-chan}
        (if started?
          (throw (Exception. "can not add new sources when already running"))
          (recur (<! in) started? (cons source-chan sources) changes?))

        {:destination id :value value}
        (do (when started?
              (doseq [source sources]
                (>! source msg)))
            (recur (<! in) started? sources true))

        {:destination id :values values}
        (do (when (and started? (not (empty? values)))
              (assert* (cfg/throttle?) "can only add bulk when throttling is enabled")
              (doseq [source sources]
                (>! source msg)))
            (recur (<! in) started? sources (not (empty? values))))

        {:started? reply-channel}
        (do (>! reply-channel started?)
            (recur (<! in) started? sources changes?))

        :heartbeat
        (do (when (and started? (cfg/throttle?))
              (doseq [source sources]
                (>! source :heartbeat)))
            (recur (<! in) started? sources false))

        :start (recur (<! in) true sources false)

        :stop (recur (<! in) false [] false)

        :reset (recur (<! in) false [] false)

        :else (recur (<! in) started? sources changes?)))
    (Coordinator. in)))

(def ^:dynamic *coordinator* (make-coordinator))

(defn started?
  []
  (let [c (chan 0)
        s (do (>!! (:in *coordinator*) {:started? c})
              (<!! c))]
    (a/close! c)
    s))