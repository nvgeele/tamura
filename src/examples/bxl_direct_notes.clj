(ns examples.bxl-direct-notes)


;; De niet zo efficiente manier

;; How do we deal with unions on key-ed sets? what if (union #{{:a 1}} #{{:a 2}}) for a set key-ed on :a?

;; What about: REACTIVE MULTISETS
;; A keyed multiset will be a unique set on its key

(comment
  ;; No implicit lifting!

  (t/defsig redis-input (t/redis "localhost" "bxlqueue"))
  (t/defsig input (map (fn [msg] msg) redis-input))

  (->> (zip input (latch input :user-id))                   ;; GLITCH WARNING!!!
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
  {:user-id   123
   :position  {:lat 0 :lon 0}
   :timestamp 1233412334}

  ;; State objects:
  {:user-id   123
   :position  {:lat 0 :lon 0}
   :direction :north
   :timestamp 1237480993}

  (t/defsig input (map parse-message (redis "localhost" "bxlqueue")))

  ;; TODO: timeout (but something something leasing...)
  (t/defsig state (reactive-hash {}
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

  (t/defsig directions (observe state {}
                                (fn [new state])
                                (fn [del state])))

  (map println directions)
  ;;;;;;;;;;;;;;;
  (r-set)
  (signal-for :user-id
              (->> (zip user-signal (delay user-signal {}))
                   (apply calculate-direction))
              merge)

  ;;;;;;;;;;;;;;;

  (t/defsig positions (redis .... :user-id))

  (t/defsig new-pos (updated positions))

  ;; removed = lease expired
  (t/defsig delayed (delay positions))

  (t/defsig previous (find (find :user-id new-pos) delayed))

  (apply calculate-direction (zip new-pos previous)))



;; We distinguish two types of reactive sets: with keys, and without keys
;; With keys we accumulate with replacements, without keys we just accumulate

;; zip will zip "matching" elements. If an element has no match, no tuple is produced for that element.

;; idea: sink nodes only can break abstraction of sets