(ns tamura.node
  (:require [clojure.core.async :as a :refer [>!! >! <!! <! go go-loop]]
            [clojure.tools.logging :as log]
            [clojure.core.match :refer [match]]
            [tamura.node-types :as nt])
  (:use [tamura.coordinator]
        [tamura.util]))

;; TODO: define some sinks
;; TODO: all sink operators are Actors, not Reactors;; make sure nothing happens when changed? = false
;; TODO: let coordinator keep list of producers, so we can have something like (coordinator-start) to kick-start errting
(defrecord Node [id node-type return-type sub-chan])
(defrecord Source [id node-type return-type sub-chan in]) ;; isa Node
(defrecord Sink [id node-type])                           ;; isa Node

(defn make-node
  [id node-type return-type sub-chan]
  (Node. id node-type return-type sub-chan))
(defn make-source
  [id node-type return-type sub-chan in]
  (Source. id node-type return-type sub-chan in))
(defn make-sink
  [id node-type]
  (Sink. id node-type))

(defmacro node-subscribe
  [source channel]
  `(>!! (:sub-chan ~source) {:subscribe ~channel}))

(def nodes (atom {}))
(def sources (atom []))
(def threads (atom []))
(def node-constructors (atom {}))

;; TODO: check that inputs aren't sinks?
(defn register-node!
  [node-type return-type args inputs]
  (let [id (new-id!)
        node {:node-type node-type
              :return-type return-type
              :args args
              :inputs inputs}]
    (swap! nodes assoc id node)
    (doseq [input-id inputs]
      (swap! nodes update-in [input-id :outputs] conj id))
    id))

(defn register-source!
  [node-type return-type args]
  (let [id (new-id!)
        node {:node-type node-type
              :return-type return-type
              :args args}]
    (swap! sources conj id)
    (swap! nodes assoc id node)
    id))

(defn register-sink!
  [node-type args inputs]
  (let [id (new-id!)
        node {:node-type node-type
              :args args
              :inputs inputs
              :sink? true}]
    (swap! nodes assoc id node)
    (doseq [input-id inputs]
      (swap! nodes update-in [input-id :outputs] conj id))
    id))

(defn get-node
  [id]
  (get @nodes id))

;; TODO: write appropriate test-code
;; TODO: build graph whilst sorting?
(defn sort-nodes
  []
  (let [visited (atom (set []))
        sorted (atom [])]
    (letfn [(sort-rec [node]
              (when-not (contains? @visited node)
                (doseq [output (:outputs (get @nodes node))]
                  (sort-rec output))
                (swap! visited conj node)
                (swap! sorted #(cons node %))))]
      (loop [roots @sources]
        (if (empty? roots)
          @sorted
          (do (sort-rec (first roots))
              (recur (rest roots))))))))

;; TODO: maybe write some tests?
;; TODO: check here that inputs aren't sinks?
(defn build-nodes!
  ([] (build-nodes! :clj))
  ([runtime]
   (loop [sorted (sort-nodes)]
     (if (empty? sorted)
       true
       (let [id (first sorted)
             node (get @nodes id)
             inputs (map #(:node (get @nodes %)) (:inputs node))
             node-obj ((get-in @node-constructors [runtime (:node-type node)]) id (:args node) inputs)]
         (swap! nodes update-in [id :node] (constantly node-obj))
         (if (nt/source? (:node-type node))
           (>!! (:in *coordinator*) {:new-source (:in node-obj)})
           (>!! (:in *coordinator*) :else))
         (recur (rest sorted)))))))

(defn register-constructor!
  [runtime node-type constructor]
  (println "register-constructor!")
  (swap! node-constructors assoc-in [runtime node-type] constructor))

(defmacro thread
  [& body]
  `(let [f# (fn [] ~@body)]
     (swap! threads conj {:body f#})))

(defmacro threadloop
  [bindings & body]
  `(thread (loop ~bindings ~@body)))

(defn subscribe-input
  [input]
  (let [c (chan)]
    (node-subscribe input c)
    c))

(defn subscribe-inputs
  [inputs]
  (map subscribe-input inputs))

;; TODO: use this in all nodes
(defmacro send-subscribers
  [subscribers changed? value id]
  `(doseq [sub# ~subscribers]
     (>! sub# {:changed? ~changed? :value ~value :from ~id})))

(defmacro subscriber-loop
  [id channel subscribers]
  `(go-loop [in# (<! ~channel)]
     (match in#
            {:subscribe c#}
            (do (log/debug (str "node " ~id " has received subscriber message"))
                (if (started?)
                  (throw (Exception. "can not add subscribers to nodes when started"))
                  (swap! ~subscribers #(cons c# %))))

            :else nil)
     (recur (<! ~channel))))