(ns examples.bxl-direct
  (:require [tamura.core :as t]))

;; No implicit lifting!

(t/defsig redis-input (t/redis "localhost" "bxlqueue"))
(t/defsig input (map (fn [msg] msg) redis-input))

(->> (zip input (latch input :user-id))                     ;; GLITCH WARNING!!!
     (filter (fn [cur prev] (not (timeout? cur prev))))
     (map (fn [cur prev] (direction (:position cur) (:position prev))))

     ;; (throttle 1)

     ;; MAGIC!

     count-by-value
     max)

;; (previous stream initial)
;; stream | (previous stream initial)
;; v1     | initial
;; v2     | v1
;; v3     | v2

;; (buffer stream trigger)

;; (buffer stream size)

;; (throttle stream time)

;; Session windowing = (buffer stream (throttle stream timeout))

;;;;;;;;;;;;;;;

;; Update messages:
{:user-id 123
 :position {:lat 0 :lon 0}
 :timestamp 1233412334}

;; State objects:
{:user-id 123
 :position {:lat 0 :lon 0}
 :direction :north
 :timestamp 1237480993}

(defsig input (map parse-message (redis "localhost" "bxlqueue")))

;; TODO: timeout (but something something leasing...)
(defsig state (reactive-hash {}
                (fn [msg state]
                  (if-let [previous (get (:user-id msg) state)]
                    (assoc state
                      (:user-id msg)
                      (assoc msg
                        :direction
                        (calculate-direction (:direction msg) (:direction previous))))
                    (assoc state
                      (:user-id msg)
                      (assoc msg :direction nil))))
                input))

(map println state)

(defsig directions (observe state {}
                            (fn [new state])
                            (fn [del state])))

(map println directions)

;;;;;;;;;;;;;;;

(defn -main
  [& args]
  (println c/hello-str))

(comment
  (constant 5)                                              ;; => constant stream
  (throttle) (buffer) (combine-latest)
  )