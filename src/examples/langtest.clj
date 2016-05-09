(ns examples.langtest
  {:lang :tamura}
  (:require [tamura.core :as t]
            [tamura.macros :refer :all]))

;; (t/def x 1)

(comment
  (t/defsig in (t/make-redis "localhost" "bxlqueue"))
  (t/map println in)
  ;;((t/lift println) in)
  )

;; (t/defn y [x] (+ x x))

(defn -main
  [& args]
  nil)