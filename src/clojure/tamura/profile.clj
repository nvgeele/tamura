(ns tamura.profile)

(def profile? false)

(defn make-wrapped
  [f time-f]
  (fn [& args]
    (let [start (. System (nanoTime))
          ret (apply f args)]
      (time-f (/ (double (- (. System (nanoTime)) start)) 1000000.0))
      ret)))

(defmacro profile
  [fn-var profile-fn]
  `(when profile?
     (alter-var-root
       (var ~fn-var)
       (fn [f#]
         (make-wrapped f# ~profile-fn)))))