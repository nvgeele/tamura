(ns tamura.node-types)

(defmacro ^{:private true} defnode-type
  [name & ancestors]
  (let [tkwd (keyword (str *ns*) (str name))
        psym (symbol (str *ns*) (str name "?"))]
    `(do
       (def ~name ~tkwd)
       (defn ~psym
         [t#]
         (or (= t# ~tkwd)
             (contains? (ancestors t#) ~tkwd)))
       (doseq [ancestor# [~@ancestors]]
         (derive ~tkwd ancestor#)))))

(defnode-type source)
(defnode-type redis source)

(defnode-type delay)
(defnode-type buffer)
(defnode-type diff-add)
(defnode-type diff-remove)

(defnode-type reduce)
(defnode-type reduce-by-key)

(defnode-type map)
(defnode-type map-by-key)

(defnode-type filter)
(defnode-type filter-by-key)
(defnode-type filter-key-size)

(defnode-type hash-to-multiset)
(defnode-type multiplicities)

(defnode-type union)
(defnode-type subtract)
(defnode-type intersection)
(defnode-type distinct)

(defnode-type do-apply)
(defnode-type throttle)
(defnode-type print)