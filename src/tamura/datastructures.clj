(ns tamura.datastructures
  (:require [clojure.data.priority-map :as pm]
            [multiset.core :as ms]))

;; TODO: leasing operations in datastructures?
;; TODO: tests for all datastructures

(defn make-hash
  ([]
   (make-hash {}))
  ([hash]
   {:type :hash :hash hash}))

(defn hash?
  [x]
  (= (:type x) :hash))

(defn hash-contains?
  [hash key]
  (let [hash (:hash hash)]
    (contains? hash key)))

(defn hash-get
  [hash key]
  (let [hash (:hash hash)]
    (get hash key)))

(defn hash-keys
  [hash]
  (let [hash (:hash hash)]
    (keys hash)))

(defn hash-insert
  [hash key val]
  (make-hash (assoc (:hash hash) key val)))

(defn hash-update
  [hash key f]
  (-> (:hash hash)
      (update key f)
      (make-hash)))

(defn hash-remove
  [hash key]
  (make-hash (dissoc (:hash hash) key)))

(defn hash->set
  [hash]
  (set (:hash hash)))

(defn make-buffered-hash)

(defn buffere-hash-insert)

(defn buffered-hash-get)

(defn make-timed-hash)

(defn timed-hash-insert)

(defn timed-hash-get)

(defn make-multiset
  ([]
   (make-multiset (ms/multiset)))
  ([multiset]
   {:type :multiset :multiset multiset}))

(defn multiset?
  [x]
  (= (:type x) :multiset))

(defn multiset-contains?
  [ms val]
  (contains? (:multiset ms) val))

(defn multiset-multiplicities
  [ms]
  (ms/multiplicities (:multiset ms)))

(defn multiset-insert
  [ms val]
  (-> (:multiset ms)
      (conj val)
      (make-multiset)))

(defn multiset-remove
  [ms val]
  (-> (:multiset ms)
      (disj val)
      (make-multiset)))

(defn multiset-minus
  [l r]
  (-> (ms/minus (:multiset l) (:multiset r))
      (make-multiset)))

(defn multiset-union
  [l r]
  (-> (ms/union (:multiset l) (:multiset r))
      (make-multiset)))

(defn make-buffered-multiset)

(defn buffered-multiset-insert)

(defn buffered-multiset-get)

(defn make-timed-multiset)

(defn timed-multiset-insert)

(defn timed-multiset-get)

(defn insert
  ([ds val]
   (case (:type ds)
     :multiset (multiset-insert ds val)
     :timed-multiset
     :buffered-multiset
     :else (throw (Exception. "Insert not defined for first argument"))))
  ([ds key val]
   (case (:type ds)
     :hash (hash-insert ds key val)
     :timed-hash (timed-hash-insert ds key val)
     :buffered-hash (buffered-hash-insert ds key val)
     :else (throw (Exception. "Insert not defined for first argument")))))