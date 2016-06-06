(defproject tamura "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/core.match "0.3.0-alpha4"]
                 [org.clojure/data.priority-map "0.0.7"]
                 [org.clojure/tools.logging "0.3.1"]
                 [clj-time "0.11.0"]
                 [amalloy/ring-buffer "1.2"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 [midje "1.8.3"]
                 [org.clojars.achim/multiset "0.1.0"]
                 [potemkin "0.4.3"]
                 [redis.clients/jedis "2.8.0"]

                 [org.apache.spark/spark-core_2.10 "1.6.1"]
                 [org.apache.spark/spark-streaming_2.10 "1.6.1"]

                 ;; deps for flambo streaming
                 [org.apache.spark/spark-streaming-kafka_2.10 "1.6.1"]
                 [org.apache.spark/spark-streaming-flume_2.10 "1.6.1"]

                 [yieldbot/flambo "0.7.1"]
                 [gorillalabs/sparkling "1.2.5"]

                 ]

  ;:injections [(require 'tamura.core)
  ;             (tamura.core/install)]

  ;; https://github.com/technomancy/leiningen/blob/master/doc/MIXED_PROJECTS.md
  :source-paths      ["src/clojure"]
  :java-source-paths ["src/java"]

  :prep-tasks [["compile" "examples.bxl-helper"]
               "javac" "compile"]

  :profiles {:dev
             {:aot [
                    ;examples.sparky
                    tamura.runtimes.spark
                    tamura.runtimes.spark-test-utils
                    examples.sparkruntime
                    ;examples.bxl-helper
                    examples.bxl-direct-spark
                    examples.bxl-bench
                    ;tamura.runtimes.spark-test
                    ]}
             :uberjar
             {:aot :all}}

  ;; :main ^:skip-aot tamura.core
  )