(defproject tamura "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [potemkin "0.4.3"]
                 [redis.clients/jedis "2.8.0"]]

  ;:injections [(require 'tamura.core)
  ;             (tamura.core/install)]

  :main ^:skip-aot tamura.core

  )