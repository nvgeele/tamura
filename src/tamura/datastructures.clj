(ns tamura.datastructures
  (:require [clojure.data.priority-map :as pm]
            [multiset.core :as ms]
            [clj-time.core :as t])
  (:import [java.util LinkedList]))

;; TODO: leasing operations in datastructures?
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
(defprotocol MultiSet
  (multiset-insert [this val])
  (multiset-remove [this val])
  (to-multiset [this])

  (multiset-contains? [this val])
  (multiset-minus [this l r])
  (multiset-union [this l r]))

(deftype RegularMultiSet [ms]
  MultiSet
  (multiset-insert [this val]
    (RegularMultiSet. (conj ms val)))
  (multiset-remove [this val]
    (RegularMultiSet. (disj ms val)))
  (to-multiset [this]
    ms)

  (multiset-contains? [this val]
    (contains? ms val))
  (multiset-minus [this l r]
    (-> (ms/minus (.ms l) (.ms r))
        (RegularMultiSet.)))
  (multiset-union [this l r]
    (-> (ms/union (.ms l) (.ms r))
        (RegularMultiSet.))))

(deftype BufferedMultiSet [ms size buffer-list]
  MultiSet
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
    (to-multiset ms))

  (multiset-contains? [this val])
  (multiset-minus [this l r])
  (multiset-union [this l r]))

(deftype TimedMultiSet [ms timeout pm]
  MultiSet
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
    (to-multiset ms))

  (multiset-contains? [this val])
  (multiset-minus [this l r])
  (multiset-union [this l r]))

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

(defn test-print [v]
  (println (to-hash v))
  v)
(defn test-print2 [v]
  (println (to-multiset v))
  v)

(comment
  (defn ttt []
    (-> (make-hash)
        (hash-insert :a 1)
        (hash-insert :b 1)
        (hash-insert :a 2)
        (hash-insert :a 3)
        (hash-insert :c 1)
        (test-print)
        (hash-remove-first :a)
        (test-print)
        (hash-remove :b)
        (test-print)
        (hash->set)))
  (defn ttt []
    (-> (make-buffered-hash 2)
        (hash-insert :a 1)
        (hash-insert :b 1)
        (hash-insert :a 2)
        (test-print)
        (hash-insert :a 3)
        (test-print)))
  (defn ttt []
    (def a (-> (make-timed-hash (t/seconds 2))
               (hash-insert :a 1)
               (hash-insert :a 2)))
    (Thread/sleep 1000)
    (def b (hash-insert a :a 3))
    (Thread/sleep 1100)
    (test-print a)
    (test-print b)
    (def c (hash-insert b :b 1))
    (test-print c))
  (defn ttt []
    (-> (make-buffered-multiset 3)
        (multiset-insert 1)
        (multiset-insert 2)
        test-print2
        (multiset-insert 3)
        (multiset-insert 4)
        test-print2
        (multiset-insert 5)
        (to-multiset)))
  (defn ttt []
    (def a (-> (make-timed-multiset (t/seconds 2))
               (multiset-insert 1)
               (multiset-insert 2)))
    (Thread/sleep 1000)
    (def b (multiset-insert a 3))
    (Thread/sleep 1100)
    (test-print2 a)
    (test-print2 b)
    (def c (multiset-insert b 4))
    (test-print2 c)))

(comment
  (declare insert)

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
    (-> (:hash hash)
        (update key #(conj (if % % []) val))
        (make-hash)))

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

  ;; TODO: error on incorrect sizes
  (defn make-buffered-hash
    ([size] (make-buffered-hash size (make-hash)))
    ([size h] {:type :buffered-hash :hash h :size size}))

  (defn buffered-hash-insert
    [h k v]
    (let [hash (:hash h)
          current (hash-get hash k)]
      (if (or (empty? current) (< (count current) (:size h)))
        (assoc h :hash (hash-insert hash k v))
        (assoc h :hash (hash-update hash k #(conj (vec (rest %)) v))))))

  (defn ttt
    []
    (-> (make-buffered-hash 2)
        (insert :a 1)
        (insert :b 1)
        (insert :a 2)
        (insert :a 3)))

  (defn buffered-hash-get
    [h key]
    (-> (:hash h)
        (hash-get key)))

  (defn make-timed-hash
    ([time] (make-timed-hash time (make-hash)))
    ([time h] {:type :buffered-hash :hash h :time time}))

  (defn timed-hash-insert
    [])

  (defn timed-hash-get
    [])

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

  (defn make-buffered-multiset [])

  (defn buffered-multiset-insert [])

  (defn make-timed-multiset [])

  (defn timed-multiset-insert []))

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