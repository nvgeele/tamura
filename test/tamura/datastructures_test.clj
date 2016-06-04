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
(defn do-fn
  [fn & args]
  (apply fn @*coll* args))

(facts "about multisets"
  (facts "about regular multisets"
    (test-with (d/make-multiset)
      (do-op d/multiset-insert 1) => (ms/multiset 1)
      (do-op d/multiset-insert 2) => (ms/multiset 1 2)
      (do-op d/multiset-insert 3) => (ms/multiset 1 2 3)
      (do-op d/multiset-insert 3) => (ms/multiset 1 2 3 3)
      (do-fn d/multiset-inserted) => [3]
      (do-op d/multiset-remove 3) => (ms/multiset 1 2 3)
      (do-fn d/multiset-removed) => [3]
      (do-op d/multiset-remove 4) => (ms/multiset 1 2 3)
      (do-fn d/multiset-removed) => []
      (do-op d/multiset-copy)
      (do-op d/multiset-insert* 1)
      (do-op d/multiset-insert* 2)
      (do-op d/multiset-insert* 3)
      (do-op d/multiset-remove* 3)
      (do-op d/multiset-remove* 2)
      (do-fn d/multiset-inserted) => [1 2 3]
      (do-fn d/multiset-removed) => [3 2]))
  (facts "about buffered multisets"
    (test-with (d/make-buffered-multiset 3)
      (do-op d/multiset-insert 1) => (ms/multiset 1)
      (do-op d/multiset-insert 2) => (ms/multiset 1 2)
      (do-op d/multiset-insert 3) => (ms/multiset 1 2 3)
      (do-op d/multiset-insert 4) => (ms/multiset 2 3 4)
      (do-fn d/multiset-removed) => [1]
      (do-op d/multiset-remove 4) => (ms/multiset 2 3)
      (do-fn d/multiset-removed) => [4]
      (do-op d/multiset-insert 2) => (ms/multiset 2 3 2)
      (do-op d/multiset-remove 2) => (ms/multiset 2 3)
      (do-op d/multiset-insert 1) => (ms/multiset 1 2 3)
      (do-op d/multiset-insert 1) => (ms/multiset 1 1 2)
      (do-fn d/multiset-removed) => [3]
      (do-op d/multiset-copy)
      (do-op d/multiset-insert* 6)
      (do-op d/multiset-insert* 7)
      (do-op d/multiset-insert* 8)
      (do-op d/multiset-remove* 8)
      (do-fn d/multiset-inserted) => [6 7 8]
      (do-fn d/multiset-removed) => [2 1 1 8]))
  (facts "about timed multisets"
    (test-with (d/make-timed-multiset (t/seconds 2))
      (do-op d/multiset-insert 1) => (ms/multiset 1)
      (Thread/sleep 1000)
      (do-op d/multiset-insert 2) => (ms/multiset 1 2)
      (Thread/sleep 1100)
      (do-op d/multiset-insert 3) => (ms/multiset 2 3)
      (Thread/sleep 3000)
      (do-op d/multiset-insert 4) => (ms/multiset 4)
      (do-fn d/multiset-removed) => (just [2 3] :in-any-order)
      (do-op d/multiset-copy)
      (Thread/sleep 2100)
      (do-op d/multiset-insert* 5)
      (do-op d/multiset-insert* 6)
      (do-fn d/multiset-inserted) => [5 6]
      (do-fn d/multiset-removed) => [4]))
  (facts "about buffered & timed multisets"
    (test-with (d/make-timed-multiset (t/seconds 2) (d/make-buffered-multiset 2))
      (do-op d/multiset-insert 1) => (ms/multiset 1)
      (do-op d/multiset-insert 2) => (ms/multiset 1 2)
      (do-op d/multiset-insert 3) => (ms/multiset 2 3)
      (do-fn d/multiset-removed) => [1]
      (Thread/sleep 1000)
      (do-op d/multiset-insert 4) => (ms/multiset 3 4)
      (Thread/sleep 1100)
      (do-op d/multiset-insert 5) => (ms/multiset 4 5)
      (Thread/sleep 2100)
      (do-op d/multiset-insert 1) => (ms/multiset 1)
      (do-fn d/multiset-removed) => (just [4 5] :in-any-order)
      (do-op d/multiset-copy)
      (Thread/sleep 2100)
      (do-op d/multiset-insert* 5)
      (do-op d/multiset-insert* 6)
      (do-op d/multiset-insert* 7)
      (do-fn d/multiset-inserted) => [5 6 7]
      (do-fn d/multiset-removed) => (just [1 5] :in-any-order))))

(facts "about hashes"
  (facts "about regular hashes"
    (test-with (d/make-hash)
      (do-op d/hash-insert :a 1) => {:a (ms/multiset 1)}
      (do-op d/hash-insert :a 2) => {:a (ms/multiset 1 2)}
      (do-fn #(d/to-multiset (d/hash-get % :a))) => (ms/multiset 1 2)
      (do-op d/hash-insert :a 3)
      (do-fn d/hash-inserted) => [[:a 3]]
      (do-op d/hash-remove-element :a 3) => {:a (ms/multiset 1 2)}
      (do-fn d/hash-removed) => [[:a 3]]
      (do-op d/hash-remove :a) => {}
      (do-fn d/hash-removed) => [[:a 1] [:a 2]]
      (do-op d/hash-insert :a 1)
      (do-op d/hash-remove-element :a 1) => {}
      (do-op d/hash-copy)
      (do-op d/hash-insert* :a 1)
      (do-op d/hash-insert* :a 2)
      (do-op d/hash-insert* :b 1)
      (do-op d/hash-remove-element* :a 2)
      (do-fn d/hash-inserted) => [[:a 1] [:a 2] [:b 1]]
      (do-fn d/hash-removed) => [[:a 2]]))
  (facts "about buffered hashes"
    (test-with (d/make-buffered-hash 2)
      (do-op d/hash-insert :a 1) => {:a (ms/multiset 1)}
      (do-op d/hash-insert :a 2) => {:a (ms/multiset 1 2)}
      (do-op d/hash-insert :a 3) => {:a (ms/multiset 2 3)}
      (do-fn d/hash-removed) => [[:a 1]]
      (do-op d/hash-insert :b 1) => {:a (ms/multiset 2 3) :b (ms/multiset 1)}
      (do-op d/hash-insert :b 2) => {:a (ms/multiset 2 3) :b (ms/multiset 1 2)}
      (do-op d/hash-insert :b 3) => {:a (ms/multiset 2 3) :b (ms/multiset 2 3)}
      (do-fn d/hash-removed) => [[:b 1]]
      (do-op d/hash-remove :a)
      (do-op d/hash-remove-element :b 2)
      (do-op d/hash-remove-element :b 3) => {}
      (do-op d/hash-copy)
      (do-op d/hash-insert* :a 1)
      (do-op d/hash-insert* :a 2)
      (do-op d/hash-insert* :b 1)
      (do-op d/hash-insert* :a 3)
      (do-op d/hash-remove-element* :a 3)
      (do-fn d/hash-inserted) => [[:a 1] [:a 2] [:b 1] [:a 3]]
      (do-fn d/hash-removed) => [[:a 1] [:a 3]]))
  (facts "about timed hashes"
    (test-with (d/make-timed-hash (t/seconds 2))
      (do-op d/hash-insert :a 1) => {:a (ms/multiset 1)}
      (Thread/sleep 1000)
      (do-op d/hash-insert :a 2) => {:a (ms/multiset 1 2)}
      (Thread/sleep 1100)
      (do-op d/hash-insert :a 3) => {:a (ms/multiset 2 3)}
      (do-fn d/hash-removed) => [[:a 1]]
      (Thread/sleep 3000)
      (do-op d/hash-insert :a 4) => {:a (ms/multiset 4)}
      (do-fn d/hash-removed) => [[:a 2] [:a 3]]
      (do-op d/hash-remove-element :a 4) => {}
      (do-op d/hash-insert :a 1)
      (do-op d/hash-insert :a 2)
      (do-op d/hash-remove :a) => {}
      (do-op d/hash-copy)
      (do-op d/hash-insert* :a 1)
      (do-op d/hash-insert* :a 2)
      (Thread/sleep 2100)
      (do-op d/hash-insert* :b 1)
      (do-fn d/hash-inserted) => [[:a 1] [:a 2] [:b 1]]
      (do-fn d/hash-removed) => (just [[:a 2] [:a 1]] :in-any-order)))
  (facts "about buffered & timed hashes"
    (test-with (d/make-timed-buffered-hash (t/seconds 2) 2)
      (do-op d/hash-insert :a 1) => {:a (ms/multiset 1)}
      (do-op d/hash-insert :a 2) => {:a (ms/multiset 1 2)}
      (do-op d/hash-insert :a 3) => {:a (ms/multiset 2 3)}
      (do-fn d/hash-removed) => [[:a 1]]
      (Thread/sleep 1000)
      (do-op d/hash-insert :a 4) => {:a (ms/multiset 3 4)}
      (Thread/sleep 1100)
      (do-op d/hash-insert :a 5) => {:a (ms/multiset 4 5)}
      (Thread/sleep 2100)
      (do-op d/hash-insert :a 1) => {:a (ms/multiset 1)}
      (do-fn d/hash-removed) => [[:a 4] [:a 5]]
      (do-op d/hash-remove-element :a 1) => {}
      (do-op d/hash-copy)
      (do-op d/hash-insert* :a 1)
      (do-op d/hash-insert* :a 2)
      (do-op d/hash-insert* :a 3)
      (do-fn d/hash-inserted) => [[:a 1] [:a 2] [:a 3]]
      (do-fn d/hash-removed) => [[:a 1]]
      (Thread/sleep 2100)
      (do-op d/hash-insert* :b 1)
      (do-fn d/hash-inserted) => [[:a 1] [:a 2] [:a 3] [:b 1]]
      (do-fn d/hash-removed) => (just [[:a 1] [:a 2] [:a 3]] :in-any-order))))