(ns tamura.values)

(defmacro deftag
  [name & ancestors]
  (let [kw (keyword (str *ns*) (str name))]
    `(do
       (def ~name ~kw)
       (doseq [ancestor# [~@ancestors]]
         (derive ~name ancestor#)))))

(deftag Value)
(deftag Constant Value)
(deftag Signal Value)
(deftag EventStream Signal)
(deftag Behaviour Signal)

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