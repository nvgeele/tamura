(ns tamura.datastructures
  (:require [clojure.data.priority-map :as pm]
            [multiset.core :as ms]
            [clj-time.core :as t]
            [amalloy.ring-buffer :as rb]))

(defn- buffer-remove-last
  [buffer size val]
  (loop [items buffer
         buffer (rb/ring-buffer size)
         switch? true]
    (cond (empty? items) buffer
          (and (= (peek items) val) switch?) (recur (pop items) buffer false)
          :else (recur (pop items) (conj buffer (peek items)) switch?))))

(defprotocol MultiSetBasic
  (multiset-insert [this val])
  (multiset-remove [this val])
  (multiset-empty? [this])
  (multiset-count [this])
  (multiset-insert-and-remove [this to-insert to-remove])
  (multiset-inserted [this])
  (multiset-removed [this])
  (to-multiset [this])
  (to-regular-multiset [this]))

(defprotocol MultiSet
  (multiset-contains? [this val])
  (multiset-minus [this r])
  (multiset-union [this r])

  ;; TODO: return dictionary?
  (multiset-multiplicities [this]))

(deftype RegularMultiSet [ms inserted removed]
  MultiSetBasic
  (multiset-insert [this val]
    (RegularMultiSet. (conj ms val) [val] []))
  (multiset-remove [this val]
    (if (contains? ms val)
      (RegularMultiSet. (disj ms val) [] [val])
      (RegularMultiSet. ms [] [])))
  (multiset-empty? [this]
    (empty? ms))
  (multiset-count [this]
    (count ms))
  (multiset-insert-and-remove [this to-insert to-remove]
    (let [to-remove (filter #(contains? ms %) to-remove)
          msi (reduce #(conj %1 %2) ms to-insert)
          msr (reduce #(disj %1 %2) msi to-remove)]
      (RegularMultiSet. msr to-insert to-remove)))
  (multiset-inserted [this]
    inserted)
  (multiset-removed [this]
    removed)
  (to-multiset [this]
    ms)
  (to-regular-multiset [this]
    this)

  MultiSet
  (multiset-contains? [this val]
    (contains? ms val))
  (multiset-minus [this r]
    (-> (ms/minus (.ms this) (.ms r))
        (RegularMultiSet. [] [])))
  (multiset-union [this r]
    (-> (ms/union (.ms this) (.ms r))
        (RegularMultiSet. [] [])))
  (multiset-multiplicities [this]
    (RegularMultiSet. (apply ms/multiset (seq (ms/multiplicities ms))) [] [])))

(deftype BufferedMultiSet [ms size buffer-list inserted removed]
  MultiSetBasic
  (multiset-insert [this val]
    (if (= (count buffer-list) size)
      (let [rel (peek buffer-list)
            msr (multiset-remove ms rel)
            msa (multiset-insert msr val)]
        (BufferedMultiSet. msa size (conj buffer-list val) (multiset-inserted msa) (multiset-removed msr)))
      (let [ms (multiset-insert ms val)]
        (BufferedMultiSet. ms size (conj buffer-list val) (multiset-inserted ms) (multiset-removed ms)))))
  (multiset-remove [this val]
    (if (multiset-contains? ms val)
      (let [ms (multiset-remove ms val)]
        (BufferedMultiSet. ms size (buffer-remove-last buffer-list size val) [] (multiset-removed ms)))
      (BufferedMultiSet. ms size buffer-list [] [])))
  (multiset-empty? [this]
    (multiset-empty? ms))
  (multiset-count [this]
    (multiset-count ms))
  (multiset-insert-and-remove [this to-insert to-remove]
    (let [[msi rmi] (reduce (fn [[ms removed] val]
                              (let [ms (multiset-insert ms val)]
                                [ms (concat removed (multiset-removed ms))]))
                            [this []]
                            to-insert)
          [msr rmr] (reduce (fn [[ms removed] val]
                              (let [ms (multiset-remove ms val)]
                                [ms (concat removed (multiset-removed ms))]))
                            [msi []]
                            to-remove)]
      (BufferedMultiSet. (.ms msr) size (.buffer-list msr) to-insert (concat rmi rmr))))
  (multiset-inserted [this]
    inserted)
  (multiset-removed [this]
    removed)
  (to-multiset [this]
    (to-multiset ms))
  (to-regular-multiset [this]
    (let [ms (to-multiset ms)]
      (RegularMultiSet. ms inserted removed))))

(deftype TimedMultiSet [ms timeout pm inserted removed]
  MultiSetBasic
  (multiset-insert [this val]
    (let [now (t/now)
          cutoff (t/minus now timeout)
          [new-ms new-pm removed]
          (loop [pm (assoc pm val now)
                 ms ms
                 removed []]
            (let [[v t] (peek pm)]
              (if (t/before? t cutoff)
                (let [ms (multiset-remove ms v)
                      pm (pop pm)]
                  (recur pm ms (concat removed (multiset-removed ms))))
                [ms pm removed])))
          ms (multiset-insert new-ms val)
          removed (concat removed (multiset-removed ms))]
      (TimedMultiSet. ms timeout new-pm [val] removed)))
  (multiset-remove [this val]
    (let [new-ms (multiset-remove ms val)
          new-pm (filter (fn [[v t]] (not (= v val))) pm)]
      (TimedMultiSet. new-ms timeout new-pm [] (multiset-removed new-ms))))
  (multiset-empty? [this]
    (multiset-empty? ms))
  (multiset-count [this]
    (multiset-count ms))
  (multiset-insert-and-remove [this to-insert to-remove]
    (let [[msi rmi] (reduce (fn [[ms removed] val]
                              (let [ms (multiset-insert ms val)]
                                [ms (concat removed (multiset-removed ms))]))
                            [this []]
                            to-insert)
          [msr rmr] (reduce (fn [[ms removed] val]
                              (let [ms (multiset-remove ms val)]
                                [ms (concat removed (multiset-removed ms))]))
                            [msi []]
                            to-remove)]
      (TimedMultiSet. (.ms msr) timeout (.pm msr) to-insert (concat rmi rmr))))
  (multiset-inserted [this]
    inserted)
  (multiset-removed [this]
    removed)
  (to-multiset [this]
    (to-multiset ms))
  (to-regular-multiset [this]
    (let [ms (to-multiset ms)]
      (RegularMultiSet. ms inserted removed))))

(defn make-multiset
  ([] (make-multiset (ms/multiset)))
  ([ms] (RegularMultiSet. ms [] [])))
(defn make-buffered-multiset
  ([size]
   (make-buffered-multiset size (make-multiset)))
  ([size ms]
   (BufferedMultiSet. ms size (rb/ring-buffer size) [] [])))
(defn make-timed-multiset
  ([timeout]
   (make-timed-multiset timeout (make-multiset)))
  ([timeout ms]
   (TimedMultiSet. ms timeout (pm/priority-map) [] [])))
(defn make-timed-buffered-multiset
  [timeout size]
  (make-timed-multiset timeout (make-buffered-multiset size)))

;; TODO: hash-get-latest
;; TODO: test for hash-filter-key-size
(defprotocol HashBasic
  (hash-get [h key])
  (hash-insert [h key val])
  (hash-remove [h key])
  (hash-remove-element [h key val])
  (hash-insert-and-remove [this to-insert to-remove])
  (hash-filter-key-size [this size])
  (hash-inserted [this])
  (hash-removed [this])
  (to-hash [h])
  (to-regular-hash [h]))

(defprotocol Hash
  (hash-update [h key f])
  (hash-contains? [h key])
  (hash-keys [this])
  (hash->set [h]))

(deftype HashImpl [hash init inserted removed]
  HashBasic
  (hash-get [h key]
    (get hash key))
  (hash-insert [h key val]
    (let [items (get hash key (init))
          new-items (multiset-insert items val)
          hash (assoc hash key new-items)
          removed (map vector (repeat key) (multiset-removed new-items))]
      (HashImpl. hash init [[key val]] removed)))
  (hash-remove [h key]
    (let [items (get hash key (init))
          hash (dissoc hash key)
          removed (map vector (repeat key) (to-multiset items))]
      (HashImpl. hash init [] removed)))
  (hash-remove-element [h key val]
    (let [items (get hash key (init))
          new-items (multiset-remove items val)
          removed (map vector (repeat key) (multiset-removed new-items))]
      (HashImpl. (if (multiset-empty? new-items)
                   (dissoc hash key)
                   (assoc hash key new-items))
                 init [] removed)))
  (hash-insert-and-remove [this to-insert to-remove]
    (let [[hi rmi] (reduce (fn [[h removed] [k v]]
                             (let [h (hash-insert h k v)]
                               [h (concat removed (hash-removed h))]))
                           [this []]
                           to-insert)
          [hr rmr] (reduce (fn [[h removed] [k v]]
                             (let [h (hash-remove-element h k v)]
                               [h (concat removed (hash-removed h))]))
                           [hi []]
                           to-remove)]
      (HashImpl. (.hash hr) init to-insert (concat rmi rmr))))
  (hash-filter-key-size [this size]
    (-> (reduce-kv #(if (>= (multiset-count %3) size) (assoc %1 %2 %3) %1) {} hash)
        (HashImpl. init [] [])))
  (hash-inserted [this]
    inserted)
  (hash-removed [this]
    removed)
  (to-hash [h]
    (reduce-kv #(assoc %1 %2 (to-multiset %3)) {} hash))
  (to-regular-hash [h]
    (HashImpl. (reduce-kv #(assoc %1 %2 (to-regular-multiset %3)) {} hash)
               make-multiset inserted removed))

  Hash
  (hash-contains? [h key]
    (contains? hash key))
  (hash-update [h key f]
    (-> (update hash key f)
        (HashImpl. init inserted removed)))
  (hash->set [h]
    (set (to-hash h)))
  (hash-keys [this]
    (keys hash)))

;; TODO: write tests for remove etc
;; TODO: do we need to filter the pm in hash-remove ?
(deftype TimedHash [hash timeout pm inserted removed]
  HashBasic
  (hash-get [h key]
    (hash-get hash key))
  (hash-insert [h key val]
    (let [now (t/now)
          cutoff (t/minus now timeout)
          [new-hash new-pm removed]
          (loop [pm (assoc pm [key val] now)
                 hash hash
                 removed []]
            (let [[[k v] t] (peek pm)]
              (if (t/before? t cutoff)
                (let [hash (hash-remove-element hash k v)
                      pm (pop pm)]
                  (recur pm hash (concat removed (hash-removed hash))))
                [hash pm removed])))
          hash (hash-insert new-hash key val)
          removed (concat removed (hash-removed hash))]
      (TimedHash. hash timeout new-pm [[key val]] removed)))
  (hash-remove [h key]
    (let [hash (hash-remove hash key)
          pm (filter (fn [[[k v] t]] (not (= k key))) pm)]
      (TimedHash. hash timeout pm [] (hash-removed hash))))
  (hash-remove-element [h key val]
    (let [hash (hash-remove-element hash key val)
          pm (filter (fn [[[k v] t]] (not (and (= k key) (= v val)))) pm)]
      (TimedHash. hash timeout pm [] (hash-removed hash))))
  (hash-insert-and-remove [this to-insert to-remove]
    (let [[hi rmi] (reduce (fn [[h removed] [k v]]
                             (let [h (hash-insert h k v)]
                               [h (concat removed (hash-removed h))]))
                           [this []]
                           to-insert)
          [hr rmr] (reduce (fn [[h removed] [k v]]
                             (let [h (hash-remove-element h k v)]
                               [h (concat removed (hash-removed h))]))
                           [hi []]
                           to-remove)]
      (TimedHash. (.hash hr) timeout (.pm hr) to-insert (concat rmi rmr))))
  ;; TODO: perform expirations?
  (hash-filter-key-size [this size]
    (-> (reduce-kv #(if (>= (multiset-count %3) size) (assoc %1 %2 %3) %1) {} hash)
        (TimedHash. timeout pm [] [])))
  (hash-inserted [this]
    inserted)
  (hash-removed [this]
    removed)
  (to-hash [h]
    ;; TODO: perform expirations?
    (to-hash hash))
  (to-regular-hash [this]
    ;; TODO: perform expirations?
    (let [rh (to-regular-hash hash)]
      (HashImpl. (.hash rh) make-multiset inserted removed)))

  Hash
  (hash-contains? [h key]
    (hash-contains? hash key))
  (hash-update [h key f]
    (hash-update hash key f))
  (hash->set [h]
    ;; TODO: Perform expirations?
    (hash->set hash))
  (hash-keys [this]
    (hash-keys hash)))

(defn make-hash
  ([] (make-hash {}))
  ([hash] (HashImpl. hash make-multiset [] [])))
(defn make-buffered-hash
  ([size] (make-buffered-hash size {}))
  ([size hash] (HashImpl. hash #(make-buffered-multiset size) [] [])))
(defn make-timed-hash
  [timeout]
  (TimedHash. (make-hash) timeout (pm/priority-map) [] []))
(defn make-timed-buffered-hash
  [timeout size]
  (TimedHash. (make-buffered-hash size) timeout (pm/priority-map) [] []))