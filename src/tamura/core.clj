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

#_(core/defn default-eval
  [form]
  (. clojure.lang.Compiler (eval form)))

(def default-eval eval)

(def default-apply apply)

(defn- my-eval
  [form]
  (println "my-eval here")
  (println (meta *ns*))
  (default-eval form))

#_(defn- my-apply
  [f & args]
  (println "my-apply here, how are you doing")
  (println (meta *ns*))
  (default-apply f args))

(core/defn spread
  {:private true
   :static true}
  [arglist]
  (cond
    (nil? arglist) nil
    (nil? (next arglist)) (seq (first arglist))
    :else (cons (first arglist) (spread (next arglist)))))

(core/defn printer
  [x]
  println
  (let [s (seq x)]
    (. (. System out) (println s))))

(core/defn my-println
  [x]
  (. (. System out) (println x)))

(core/defn my-apply
  "Applies fn f to the argument list formed by prepending intervening arguments to args."
  {:added "1.0"
   :static true}

  ([^clojure.lang.IFn f args]

   (my-println (str "Function: " f))
   (my-println "Arguments:")
   (doseq [arg args]
     (my-println arg))
   (my-println "----------")

   (. f (applyTo (seq args))))

  ([^clojure.lang.IFn f x args]

   (my-println (str "Function: " f))
   (my-println "----------")

   (. f (applyTo (list* x args))))

  ([^clojure.lang.IFn f x y args]

   (my-println (str "Function: " f))
   (my-println "----------")

   (. f (applyTo (list* x y args))))

  ([^clojure.lang.IFn f x y z args]

   (my-println (str "Function: " f))
   (my-println "----------")

   (. f (applyTo (list* x y z args))))

  ([^clojure.lang.IFn f a b c d & args]

   (my-println (str "Function: " f))
   (my-println "----------")

   (. f (applyTo (cons a (cons b (cons c (cons d (spread args)))))))))

(core/defn set-eval
  []
  (alter-var-root #'eval (constantly #'my-eval)))

(core/defn set-apply
  []
  (alter-var-root
    #'apply
    (constantly my-apply)
    #_(fn [f]
        #(do (println "woi")
             (default-apply %1 %2)))))

(defmacro install
  "Installs"
  []
  ;; Check if :lang :tamura in metadata?
  ;; Overwrite eval?
  ;; Overwrite macros?

  ;; (println (str *ns*))
  ;; (println (meta *ns*))
  (println "Installing tamura...")
  ;; (println #'eval)
  ;; (alter-var-root #'eval (constantly #'my-eval))
  ;; (alter-var-root #'apply (constantly #'my-apply))

  ;; (set-apply)

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
    (eval r)
    ))