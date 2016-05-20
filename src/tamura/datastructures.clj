(ns tamura.datastructures
  (:require [clojure.data.priority-map :as pm]
            [multiset.core :as ms]
            [clj-time.core :as t])
  (:import [java.util LinkedList]))

;; TODO: tests for all datastructures

;; TODO: to-multiset
(defprotocol MultiSetBasic
  (multiset-insert [this val])
  (multiset-remove [this val])
  (to-multiset [this]))

(defprotocol MultiSet
  (multiset-contains? [this val])
  (multiset-minus [this l r])
  (multiset-union [this l r])

  ;; TODO: return dictionary?
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
    (if (multiset-contains? ms val)
      (-> (multiset-remove ms val)
          (BufferedMultiSet. size (do (.removeLastOccurrence buffer-list val)
                                      buffer-list)))
      this))
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

(defn make-multiset
  ([] (make-multiset (ms/multiset)))
  ([ms] (RegularMultiSet. ms)))
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

;; TODO: hash-get-latest
(defprotocol HashBasic
  (hash-get [h key])
  (hash-insert [h key val])
  (hash-remove [h key])
  (hash-remove-element [h key val])
  (to-hash [h]))

(defprotocol Hash
  (hash-update [h key f])
  (hash-contains? [h key])
  (hash-keys [this])
  (hash->set [h]))

(deftype HashImpl [init hash]
  HashBasic
  (hash-get [h key]
    (get hash key))
  (hash-insert [h key val]
    (hash-update h key #(multiset-insert (if % % (init)) val)))
  (hash-remove [h key]
    (->> (dissoc hash key)
         (HashImpl. init)))
  (hash-remove-element [h key val]
    (let [items (get hash key (init))
          new-items (multiset-remove items val)]
      (HashImpl. init (assoc hash key new-items))))
  (to-hash [h]
    (reduce-kv #(assoc %1 %2 (to-multiset %3)) {} hash))

  Hash
  (hash-contains? [h key]
    (contains? hash key))
  (hash-update [h key f]
    (->> (update hash key f)
         (HashImpl. init)))
  (hash->set [h]
    (set (to-hash h)))
  (hash-keys [this]
    (keys hash)))

(defn make-hash
  ([] (make-hash {}))
  ([hash] (HashImpl. make-multiset hash)))
(defn make-buffered-hash
  ([size] (make-buffered-hash size {}))
  ([size hash] (HashImpl. #(make-buffered-multiset size) hash)))
(defn make-timed-hash
  ([timeout] (make-timed-hash timeout {}))
  ([timeout hash] (HashImpl. #(make-timed-multiset timeout) {})))
(defn make-timed-buffered-hash
  ([timeout size] (make-timed-buffered-hash timeout size {}))
  ([timeout size hash] (HashImpl. #(make-timed-multiset timeout (make-buffered-multiset size)) {})))