(ns examples.bxl-direct
  (:require [tamura.core :as t]
            [examples.spawner :as s]))

;;;;;;;;;;;;;;;

(defn calculate-direction
  [[cur_lat cur_lon] [pre_lat pre_lon]]
  (let [y (* (Math/sin (- cur_lon pre_lon)) (Math/cos cur_lat))
        x (- (* (Math/cos pre_lat) (Math/sin cur_lat))
             (* (Math/sin pre_lat) (Math/cos cur_lat) (Math/cos (- cur_lon pre_lon))))
        bearing (Math/atan2 y x)
        deg (mod (+ (* bearing (/ 180.0 Math/PI)) 360) 360)]
    (cond (and (>= deg 315.) (<= deg 45.)) :east
          (and (>= deg 45.) (<= deg 135.)) :north
          (and (>= deg 135.) (<= deg 225.)) :west
          :else :south)))

(t/defsig positions (t/redis "localhost" "bxlqueue" :key :user-id))
;(t/print-signal positions)

(t/defsig old-positions (t/delay positions))
;(t/print-signal old-positions)

(t/defsig updates (t/zip positions old-positions))
;(t/print-signal updates)

(t/defsig directions (t/map-to-multiset (fn [[user-id [new old]]]
                                          (calculate-direction (:position new) (:position old)))
                                        updates))
;(t/print-signal directions)

(t/defsig direction-count (t/multiplicities directions))
;(t/defsig direction-count (t/multiplicities (t/throttle directions 1000)))
;(t/print-signal direction-count)

(t/defsig max-direction (t/reduce #(if (> (second %1) (second %2)) %1 %2)
                                  [nil -1]
                                  direction-count))
;(t/print-signal max-direction)

(t/print-signal (t/throttle max-direction 1000))
;(t/print-signal max-direction)

;;;;;;;;;;;;;;;

(defn -main
  [& args]
  (t/start)
  (s/spawn-threads "localhost" "bxlqueue" 10)
  (println "Ready"))