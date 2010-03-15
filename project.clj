(defproject com.ashafa/nozzle "0.1.0-SNAPSHOT" 
  :description "A Clojure library for streaming stauses from the Twitter Streaming API." 
  :dependencies [[org.clojure/clojure "1.1.0-master-SNAPSHOT"]
                 [org.clojure/clojure-contrib "1.0-SNAPSHOT"]
                 [jline "0.9.9"]]
  :dev-dependencies [[autodoc "0.7.0"]
                     [leiningen/lein-swank "1.1.0"]
                     [lein-clojars "0.5.0-SNAPSHOT"]]
  :source-path "src/main/clojure"
  :resources-path "src/main/resources"
  :test-path "src/test/clojure;src/main/clojure")
