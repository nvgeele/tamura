(ns examples.langtest
  {:lang :tamura}
  (:require [tamura.core :as t]))

;; (t/def x 1)

(def x 1)

(defn -main
  [& args]

  (t/set-apply)

  (print "gif me: ") (flush)
  (let [input (read-line)
        num (Integer. input)]
    (println (* num num)))

  ;; (println (* (+ x x) (+ x x)))
  ;; (println s)
  )