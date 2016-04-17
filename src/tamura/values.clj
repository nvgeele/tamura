(ns tamura.values
  (:require [clojure.string :as str]))

(defprotocol TaggedValue
  (tag [this])
  (value [this]))

(deftype TValue [tag value]
  TaggedValue
  (tag [this] tag)
  (value [this] value))

(defn make-tagged
  [tag value]
  (TValue. tag value))

(defmacro deftag
  [name & ancestors]
  (let [tkwd (keyword (str *ns*) (str name))
        psym (symbol (str *ns*) (str (str/lower-case (str name)) "?"))
        fsym (symbol (str *ns*) (str "make-" (str/lower-case (str name))))]
    `(do
       (def ~name ~tkwd)
       (defn ~fsym
         [v#]
         (make-tagged ~tkwd v#))
       (defn ~psym
         [v#]
         (and (= (type v#) TValue)
              (isa? (tag v#) ~tkwd)))
       (doseq [ancestor# [~@ancestors]]
         (derive ~name ancestor#)))))

(deftag Value)
(deftag Constant Value)
(deftag Signal Value)
(deftag EventStream Signal)
(deftag Behaviour Signal)

(defprotocol Reactor
  (current [this])
  (next [this]))