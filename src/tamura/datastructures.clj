(ns tamura.datastructures
  (:require [clojure.data.priority-map :as pm]
            [multiset.core :as ms]
            [clj-time.core :as t])
  (:import [java.util LinkedList]))

;; TODO: tests for all datastructures

;; TODO: hash-get-latest
(defprotocol Hash
  (hash-insert [h key val])
  (hash-get [h key])
  (hash-contains? [h key])
  (hash-update [h key f])
  (hash-remove [h key])
  (hash-remove-element [h key val])
  (hash-remove-first [h key])
  (hash->set [h])
  (to-hash [h]))

(deftype RegularHash [hash]
  Hash
  (hash-update [h key f]
    (-> (update hash key f)
        (RegularHash.)))
  (hash-insert [h key val]
    (hash-update h key #(conj (if % % []) val)))
  (hash-get [h key]
    (get hash key))
  (hash-contains? [h key]
    (contains? hash key))
  (hash-remove [h key]
    (-> (dissoc hash key)
        (RegularHash.)))
  (hash-remove-element [h key val]
    (let [items (get hash key)
          new-items (remove #{val} items)]
      (RegularHash. (assoc hash key (vec new-items)))))
  (hash-remove-first [h key]
    (hash-update h key #(vec (rest %))))
  (hash->set [h]
    (set hash))
  (to-hash [h]
    hash))

;; NOTE: must either contain TimedHash or RegularHash
(deftype BufferedHash [hash size]
  Hash
  (hash-insert [h key val]
    (let [current (hash-get hash key)
          new-hash (if (or (empty? current) (< (count current) size))
                     (hash-insert hash key val)
                     (-> (hash-remove-first hash key)
                         (hash-insert key val)))]
      (BufferedHash. new-hash size)))
  (hash-get [h key]
    (hash-get hash key))
  (hash-contains? [h key]
    (hash-contains? hash key))
  (hash-update [h key f]
    (-> (hash-update hash key f)
        (BufferedHash. size)))
  (hash-remove [h key]
    (-> (hash-remove hash key)
        (BufferedHash. size)))
  (hash-remove-element [h key val]
    (-> (hash-remove-element hash key val)
        (BufferedHash. size)))
  (hash-remove-first [h key]
    (-> (hash-remove-first hash key)
        (BufferedHash. size)))
  (hash->set [h]
    (hash->set hash))
  (to-hash [h]
    (to-hash hash)))

;; NOTE: must either contain BufferedHash or RegularHash
(deftype TimedHash [hash timeout pm]
  Hash
  (hash-insert [h key val]
    (let [now (t/now)
          cutoff (t/minus now timeout)
          [new-hash new-pm]
          (loop [pm (assoc pm [key val] now)
                 hash hash]
            (let [[[k v] t] (peek pm)]
              (if (t/before? t cutoff)
                (let [hash (hash-remove-element hash k v)
                      pm (pop pm)]
                  (recur pm hash))
                [hash pm])))]
      (-> (hash-insert new-hash key val)
          (TimedHash. timeout new-pm))))
  (hash-get [h key]
    (hash-get hash key))
  (hash-contains? [h key]
    (hash-contains? hash key))
  (hash-update [h key f]
    (-> (hash-update hash key f)
        (TimedHash. timeout pm)))
  (hash-remove [h key]
    (let [hash (hash-remove hash key)
          pm (filter (fn [[[k v] t]] (not (= k key))) pm)]
      (TimedHash. hash timeout pm)))
  (hash-remove-element [h key val]
    (let [hash (hash-remove hash key)
          pm (filter (fn [[[k v] t]] (not (and (= k key) (= v val)))) pm)]
      (TimedHash. hash timeout pm)))
  (hash-remove-first [h key]
    (let [val (first (hash-get hash key))
          hash (hash-remove-first hash key)
          pm (filter (fn [[[k v] t]] (not (and (= k key) (= v val)))) pm)]
      (TimedHash. hash timeout pm)))
  (hash->set [h]
    (set hash))
  (to-hash [h]
    (to-hash hash)))

;; TODO: to-multiset
(defprotocol MultiSetBasic
  (multiset-insert [this val])
  (multiset-remove [this val])
  (to-multiset [this]))

(defprotocol MultiSet
  (multiset-contains? [this val])
  (multiset-minus [this l r])
  (multiset-union [this l r])
  (multiset-multiplicities [this]))

(deftype RegularMultiSet [ms]
  MultiSetBasic
  (multiset-insert [this val]
    (RegularMultiSet. (conj ms val)))
  (multiset-remove [this val]
    (RegularMultiSet. (disj ms val)))
  (to-multiset [this]
    ms)

  MultiSet
  (multiset-contains? [this val]
    (contains? ms val))
  (multiset-minus [this l r]
    (-> (ms/minus (.ms l) (.ms r))
        (RegularMultiSet.)))
  (multiset-union [this l r]
    (-> (ms/union (.ms l) (.ms r))
        (RegularMultiSet.)))
  (multiset-multiplicities [this]
    (ms/multiplicities ms)))

(deftype BufferedMultiSet [ms size buffer-list]
  MultiSetBasic
  (multiset-insert [this val]
    (let [new-ms (if (= (count buffer-list) size)
                   (let [rel (.removeLast buffer-list)]
                     (multiset-insert (multiset-remove ms rel) val))
                   (multiset-insert ms val))]
      (.addFirst buffer-list val)
      (BufferedMultiSet. new-ms size buffer-list)))
  (multiset-remove [this val]
    (-> (multiset-remove ms val)
        (BufferedMultiSet. size buffer-list)))
  (to-multiset [this]
    (to-multiset ms)))

(deftype TimedMultiSet [ms timeout pm]
  MultiSetBasic
  (multiset-insert [this val]
    (let [now (t/now)
          cutoff (t/minus now timeout)
          [new-ms new-pm]
          (loop [pm (assoc pm val now)
                 ms ms]
            (let [[v t] (peek pm)]
              (if (t/before? t cutoff)
                (let [ms (multiset-remove ms v)
                      pm (pop pm)]
                  (recur pm ms))
                [ms pm])))]
      (-> (multiset-insert new-ms val)
          (TimedMultiSet. timeout new-pm))))
  (multiset-remove [this val]
    (let [new-ms (multiset-remove ms val)
          new-pm (filter (fn [[v t]] (not (= v val))) pm)]
      (TimedMultiSet. new-ms timeout new-pm)))
  (to-multiset [this]
    (to-multiset ms)))

(defn make-hash
  ([] (make-hash {}))
  ([hash] (RegularHash. hash)))
(defn make-buffered-hash
  ([size] (make-buffered-hash size (make-hash)))
  ([size hash] (BufferedHash. hash size)))
(defn make-timed-hash
  ([timeout] (make-timed-hash timeout (make-hash)))
  ([timeout hash] (TimedHash. hash timeout (pm/priority-map))))

(defn make-multiset
  []
  (RegularMultiSet. (ms/multiset)))
(defn make-buffered-multiset
  ([size]
   (make-buffered-multiset size (make-multiset)))
  ([size ms]
   (BufferedMultiSet. ms size (LinkedList.))))
(defn make-timed-multiset
  ([timeout]
    (make-timed-multiset timeout (make-multiset)))
  ([timeout ms]
   (TimedMultiSet. ms timeout (pm/priority-map))))