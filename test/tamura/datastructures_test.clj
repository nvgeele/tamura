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
    (test-with (d/make-timed-multiset (t/seconds 2))
      (do-op d/multiset-insert 1) => (ms/multiset 1)
      (Thread/sleep 1000)
      (do-op d/multiset-insert 2) => (ms/multiset 1 2)
      (Thread/sleep 1100)
      (do-op d/multiset-insert 3) => (ms/multiset 2 3)
      (Thread/sleep 3000)
      (do-op d/multiset-insert 4) => (ms/multiset 4)))
  (facts "about buffered & timed multisets"
    (test-with (d/make-timed-multiset (t/seconds 2) (d/make-buffered-multiset 2))
      (do-op d/multiset-insert 1) => (ms/multiset 1)
      (do-op d/multiset-insert 2) => (ms/multiset 1 2)
      (do-op d/multiset-insert 3) => (ms/multiset 2 3)
      (Thread/sleep 1000)
      (do-op d/multiset-insert 4) => (ms/multiset 3 4)
      (Thread/sleep 1100)
      (do-op d/multiset-insert 5) => (ms/multiset 4 5)
      (Thread/sleep 2100)
      (do-op d/multiset-insert 1) => (ms/multiset 1))))

(facts "about hashes"
  (facts "about regular hashes"
    (test-with (d/make-hash)
      (do-op d/hash-insert :a 1) => {:a (ms/multiset 1)}
      (do-op d/hash-insert :a 2) => {:a (ms/multiset 1 2)}
      (d/to-multiset (d/hash-get @*coll* :a)) => (ms/multiset 1 2)
      (do-op d/hash-insert :a 3)
      (do-op d/hash-remove-element :a 3) => {:a (ms/multiset 1 2)}
      (do-op d/hash-remove :a) => {}))
  (facts "about buffered hashes"
    (test-with (d/make-buffered-hash 2)
      (do-op d/hash-insert :a 1) => {:a (ms/multiset 1)}
      (do-op d/hash-insert :a 2) => {:a (ms/multiset 1 2)}
      (do-op d/hash-insert :a 3) => {:a (ms/multiset 2 3)}
      (do-op d/hash-insert :b 1) => {:a (ms/multiset 2 3) :b (ms/multiset 1)}
      (do-op d/hash-insert :b 2) => {:a (ms/multiset 2 3) :b (ms/multiset 1 2)}
      (do-op d/hash-insert :b 3) => {:a (ms/multiset 2 3) :b (ms/multiset 2 3)}))
  (facts "about timed hashes"
    (test-with (d/make-timed-hash (t/seconds 2))
      ))
  (facts "about buffered & timed hashes"
    (test-with (d/make-timed-buffered-hash (t/seconds 2) 2)
      )))