(ns tamura.datastructures-test
  (:use midje.sweet)
  (:require [tamura.datastructures :as d]
            [multiset.core :as ms]
            [clj-time.core :as t]))

(def ^:dynamic *coll*)
(defmacro test-with
  [coll & body]
  `(binding [*coll* (atom ~coll)]
     ~@body))
(defn do-op
  [& args]
  (let [coll (apply swap! *coll* args)]
    (if (satisfies? d/MultiSetBasic coll)
      (d/to-multiset coll)
      (d/to-hash coll))))

(facts "about multisets"
  (facts "about regular multisets"
    (test-with (d/make-multiset)
      (do-op d/multiset-insert 1) => (ms/multiset 1)
      (do-op d/multiset-insert 2) => (ms/multiset 1 2)
      (do-op d/multiset-insert 3) => (ms/multiset 1 2 3)
      (do-op d/multiset-insert 3) => (ms/multiset 1 2 3 3)
      (do-op d/multiset-remove 3) => (ms/multiset 1 2 3)
      (do-op d/multiset-remove 4) => (ms/multiset 1 2 3)))
  (facts "about buffered multisets"
    (test-with (d/make-buffered-multiset 3)
      (do-op d/multiset-insert 1) => (ms/multiset 1)
      (do-op d/multiset-insert 2) => (ms/multiset 1 2)
      (do-op d/multiset-insert 3) => (ms/multiset 1 2 3)
      (do-op d/multiset-insert 4) => (ms/multiset 2 3 4)

      (do-op d/multiset-remove 4) => (ms/multiset 2 3)
      (do-op d/multiset-insert 2) => (ms/multiset 2 3 2)

      (do-op d/multiset-remove 2) => (ms/multiset 2 3)
      (do-op d/multiset-insert 1) => (ms/multiset 1 2 3)
      (do-op d/multiset-insert 1) => (ms/multiset 1 1 2)))
  (facts "about timed multisets"
    )
  (facts "about buffered & timed multisets"
    ))

(facts "about hashes"
  (facts "about regular hashes")
  (facts "about buffered hashes")
  (facts "about timed hashes")
  (facts "about buffered & timed hashes"))