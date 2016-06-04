(ns tamura.coordinator
  (:require [clojure.core.async :as a :refer [>!! >! <!! <! go go-loop]]
            [clojure.core.match :refer [match]]
            [clojure.tools.logging :as log])
  (:use [tamura.util]))

(defrecord Coordinator [in])

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

(def ^:dynamic *coordinator* (make-coordinator))

(defn started?
  []
  (let [c (chan 0)
        s (do (>!! (:in *coordinator*) {:started? c})
              (<!! c))]
    (a/close! c)
    s))