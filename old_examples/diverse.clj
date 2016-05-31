(ns examples.diverse
  (:require [tamura.core :as t]
            [clojure.string :as cstr]))

(t/defsig stateful-word-countcount
          (->> (t/redis "localhost" "sentences")
               (t/flatmap-to-multiset (fn [sentence] (cstr/split sentence " ")))
               (t/multiplicities)
               ;; Optionally transform the multiplicties to a dictionary
               (t/map-to-hash identity)))

(t/defsig stateful-hashtag-count
          (->> (t/twitter)
               (t/flatmap-to-multiset (fn [tweet] (extract-hashtags tweet)))
               (t/multiplicities)
               ;; Optionally transform the multiplicities to a dictionary
               (t/map-to-hash identity)))