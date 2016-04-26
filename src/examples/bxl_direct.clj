(ns examples.bxl-direct
  (:require [tamura.core :as t]))

;;;;;;;;;;;;;;;

;; DELAY
(comment
  ;; How it should be (for keyed sets)
  #{{:id 1 :v 1}}
  #{}

  #{{:id 1 :v 1} {:id 2 :v 1}}
  #{}

  #{{:id 1 :v 2} {:id 2 :v 1}}
  #{{:id 1 :v 1}}

  ;; How it should be (for none keyed sets)
  #{a}
  #{}

  #{a b}
  #{a}

  #{a b c}
  #{a b})

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

(defmacro print-signal
  [signal]
  `(t/do-apply #(println (str (quote ~signal) ": " %)) ~signal))

(t/defsig positions (t/redis "localhost" "bxlqueue" :key :user-id))
;(print-signal positions)

(t/defsig old-positions (t/delay positions))
;(print-signal old-positions)

(t/defsig updates (t/zip positions old-positions))
;(print-signal updates)

(t/defsig directions (t/map (fn [[new old]]
                              (calculate-direction (:position new) (:position old)))
                            updates))
;(print-signal directions)

(t/defsig direction-count (t/multiplicities directions))
;(t/defsig direction-count (t/multiplicities (t/throttle directions 1000)))
;(print-signal direction-count)

(t/defsig max-direction (t/reduce (fn [l r] (if (> (second l) (second r)) l r)) direction-count))
;(print-signal max-direction)

(print-signal (t/throttle max-direction 1000))
;(print-signal max-direction)

;; TODO: minimise node boilerplate
;; TODO: buffer
;; TODO: leasing
;; TODO: filter node
;; TODO: betere primitives zodat het duidelijker is wat wat nu juist maakt
;; TODO: meer examples
;; TODO: waarom sets juist?
;; TODO: static architecture

;;;;;;;;;;;;;;;

(defn -main
  [& args]
  (println "Blank!"))