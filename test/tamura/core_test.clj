(ns tamura.core-test
  (:use midje.sweet)
  (:require [tamura.core :as core]
            [clojure.core.async :as a :refer [>!! <!!]]))

(defn make-test-input
  [])

(facts
  "about make-delay-node"
  (facts
    "about make-delay-node with multisets"
    (let [source-node (core/make-source-node)
          delay-node (core/make-delay-node source-node)
          test-channel (core/chan)]
      (core/node-subscribe delay-node test-channel)

      (println (:in source-node))

      :truth => :truth

      ;(>!! test-channel {:destination})

      )
    )
  (facts
    "about make-delay-node with hashes"
    ))