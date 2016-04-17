(ns tamura.core
  (:require [potemkin :as p]
            [tamura.macros :as macros]
            [tamura.values :as values]
            [tamura.funcs :as funcs]
            [clojure.core :as core]))

(p/import-vars
  [tamura.macros

   def
   defn
   defsig]

  [tamura.funcs

   map])

(defmacro install
  "Installs"
  []
  ;; Check if :lang :tamura in metadata?
  ;; Overwrite eval?
  ;; Overwrite macros?

  (println "Installing tamura...")
  nil)

(core/defn -main
  [& args]
  #_(when (not (= (count args) 1))
    (println "Provide a source file please")
    (System/exit 1))
  (println "Go:") (flush)
  (let [r (read)
        form `(do (ns tamura.read-code
                    (:require [tamura.core :refer :all]
                              [tamura.macros :refer :all]))
                  ~r)]
    ;; (println (macroexpand form))
    (eval r)))