(ns examples.sparkling
  (:require [sparkling.conf :as conf]
            [sparkling.core :as spark]
            [sparkling.serialization]
            [sparkling.destructuring :as s-de]
            [clojure.reflect :as r]
            [clojure.pprint :refer [pprint print-table]]))

(comment
  (doseq [m (sort (map :name (:members (r/reflect "data"))))] (println m))
  (let [methods (sort #(compare (.getName %1) (.getName %2)) (.getMethods (type data)))]
    (doseq [m methods]
      (println "Method Name: " (.getName m))
      (println "Return Type: " (.getReturnType m) "\n"))))

(comment
  (let [sc (-> (conf/spark-conf)
               (conf/master "local[*]")
               (conf/app-name "tfidf")
               (spark/spark-context))
        ;data (spark/parallelize-pairs sc [["a" 1] ["b" 2] ["c" 3] ["d" 4] ["e" 5]])
        data (spark/parallelize-pairs sc [(spark/tuple :a 1)
                                          (spark/tuple :b 2)])
        ]
    (-> data
        ;(spark/collect)
        (spark/collect-map)
        ;(type)
        ;(.collectAsMap)
        ;(.collectAsMap)
        (pprint))))

(comment
  JavaStreamingContext ssc = new JavaStreamingContext(sc,
                                                       Durations.seconds(1))

  ssc.checkpoint(checkpointDir);

  JavaDStream<UserPosition> positions = ssc.receiverStream(new RedisReceiver()))

(defn -main
  [& args]
  (println "Whatevs"))