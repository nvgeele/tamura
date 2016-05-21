(ns tamura.semantics)

;; TODO: what about mapping over all values for a key
;; TODO: what about reducing
(comment reduce-per-key)
(comment hash-values)

;; TODO: buffer on key
;; NOTE: if the collections are *fully* reactive, and the hash becomes too big (as in too many keys), sharding and
;; saving shards to persistent storage might be feasible
(comment "Semantics source with buffering (size 3)"
  "multiset"
  1 => #{1}
  2 => #{1 2}
  3 => #{1 2 3}
  4 => #{2 3 4}

  "hash"
  [:a 1] => {:a #{1}}
  [:b 1] => {:a #{1} :b #{1}}
  [:a 2] => {:a #{1 2} :b #{1}}
  [:a 3] => {:a #{1 2 3} :b #{1}}
  [:a 4] => {:a #{2 3 4} :b #{1}}
  [:c 1] => {:a #{2 3 4} :b #{1} :c #{1}}
  [:d 1] => {:a #{2 3 4} :b #{1} :c #{1} :d #{1}})

(comment "Semantics source with time-based leasing (10 seconds)"
  "multiset"
  1 => #{1}
  2 => #{1 2}
  - 10 seconds wait -
  3 => #{3}

  "hash"
  [:a 1] => {:a #{1}}
  [:b 1] => {:a #{1} :b #{1}}
  [:a 2] => {:a #{1 2} :b #{1}}
  - 10 seconds wait -
  [:a 3] => {:a #{3}}
  - 5 seconds wait -
  [:a 4] => {:a #{3 4}}
  - 5 seconds wait -
  [:a 5] => {:a #{4 5}})

(comment "Semantics buffer (size 2)"
  "multiset"
  #{1}       => #{1}
  #{1 2}     => #{1 2}
  #{1 2 3}   => #{2 3}
  #{1 2 3 4} => #{3 4}

  "hash"
  {:a #{1}}       => {:a #{1}}
  {:a #{1 2}}     => {:a #{1 2}}
  {:a #{1 2 3}}   => {:a #{2 3}}
  {:a #{1 2 3 4}} => {:a #{3 4}})

(comment
  "Semantics buffer of size 2, multiset, after leasing/buffer"
  #{1}        => #{1}
  #{1 2}      => #{1 2}
  #{1 2 3}    => #{2 3}
  - timeout -
  #{4}        => #{4}
  #{4 5}      => #{4 5}

  "Semantics buffer of size 3, multiset, after buffered source of size 2"
  #{1}   => #{1}
  #{1 2} => #{1 2}
  #{2 3} => #{2 3}
  #{3 4} => #{3 4}

  "Semantics buffer of size 2, hash, after leasing/buffer"
  {:a #{1}}             => {:a #{1}}
  {:a #{1 2}}           => {:a #{1 2}}
  {:a #{1 2 3}}         => {:a #{2 3}}
  {:a #{1 2 3} :b #{1}} => {:a #{2 3} :b #{1}}
  - timeout for :a -
  {:d #{1}}             => {:d #{1}}
  {:d #{1} :e #{1}}     => {:d #{1} :e #{1}})

(comment "Semantics delay, after non-leased/buffered source"
  "multiset"
  #{1}     => #{}
  #{1 2}   => #{1}
  #{1 2 3} => #{1 2}

  "hash"
  {:a #{1}}           => {}
  {:a #{1} :b #{1}}   => {:a #{1}}
  {:a #{1 2} :b #{1}} => {:a #{1} :b #{1}})

(comment "Semantics delay, after leasing/buffer"
  "multiset (buffer size 3)"
  #{1}        => #{}
  #{1 2}      => #{1}
  #{1 2 3}    => #{1 2}
  #{2 3 4}    => #{1 2 3}
  #{2 3 4 5}  => #{2 3 4}

  "hash (buffer size 2)"
  {:a #{1}}                   => {}
  {:a #{1 2}}                 => {:a #{1}}
  {:a #{1 2} :b #{1}}         => {:a #{1 2}}
  {:a #{1 2} :b #{1 2}}       => {:a #{1 2} :b #{1}}
  - timeout for :a -
  {:b #{1 2} :c #{1}}         => {:a #{1 2} :b #{1 2}}
  {:b #{1 2} :c #{1} :a #{3}} => {:b #{1 2} :c #{1}}
  {:b #{2 3} :c #{1} :a #{3}} => {:b #{1 2} :c #{1} :a #{3}})

(comment "Semantics source with time-based leasing (10 seconds)"
  "multiset"

  "hash")

(comment "Semantics map-to-hash"
  "multiset"

  "hash")

(comment "Semantics map-to-multiset"
  "multiset"

  "hash")

(comment "Semantics do-apply"
  "multiset"

  "hash")

;; TODO: filter specific for hash (filter-keys? and filter-values)?
(comment filter-per-key filter-key)

(comment "Semantics filter"
  "multiset"

  "hash")

(comment "Semantics zip"
  "multiset"

  "hash")

;; TODO: return hash instead of ?
(comment "Semantics multiplicities"
  "multiset"

  "hash")

(comment "Semantics reduce"
  "multiset"

  "hash")

;; TODO: something special for hashes called previous?

;; TODO: operation to restrict number of keys?