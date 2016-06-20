(ns tamura.node-types
  (:refer-clojure :exclude [delay delay? reduce map map? filter distinct distinct? print send]))

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

;;;;           SOURCES           ;;;;

(defnode-type source)
(defnode-type redis source)

;;;;  "POLYMORPHIC" OPERATIONS   ;;;;

(defnode-type delay)
(defnode-type buffer)
(defnode-type diff-add)
(defnode-type diff-remove)
(defnode-type do-apply)
(defnode-type throttle)
(defnode-type print)
(defnode-type redis-out)

;;;;     MULTISET OPERATIONS     ;;;;

(defnode-type map)
(defnode-type reduce)
(defnode-type filter)
(defnode-type multiplicities)
(defnode-type union)
(defnode-type subtract)
(defnode-type intersection)
(defnode-type distinct)

;;;;       HASH OPERATIONS       ;;;;

(defnode-type map-by-key)
(defnode-type reduce-by-key)
(defnode-type filter-by-key)
(defnode-type filter-key-size)
(defnode-type hash-to-multiset)