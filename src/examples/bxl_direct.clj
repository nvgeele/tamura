(ns examples.bxl-direct
  (:require [tamura.core :as c]))

;; No implicit lifting!

(defsig redis-input (redis "localhost" "bxlqueue"))
(defsig input (map (fn [msg] msg) redis-input))

(->> (zip input (latch input :user-id))                     ;; GLITCH WARNING!!!
     (filter (fn [cur prev] (not (timeout? cur prev))))
     (map (fn [cur prev] (direction (:position cur) (:position prev))))

     (throttle 1)

     count-by-value
     max)

(defn -main
  [& args]
  (println c/hello-str))

(comment
  (constant 5)                                              ;; => constant stream
  (throttle) (buffer) (combine-latest)
  )