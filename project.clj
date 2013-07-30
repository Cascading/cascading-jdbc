(defproject cascading/cascading-provider-jdbc "1.0-wip-dev"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :url "http://github.com/Cascading/maple"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :description "All the Cascading taps we have to offer."
  :repositories {"conjars" "http://conjars.org/repo/"}
  :profiles {:provided {:dependencies [[org.apache.hbase/hbase "0.94.5"
                                        :exclusions [org.apache.hadoop/hadoop-core asm]]]}
             :dev {:dependencies [[midje "1.5.0" :exclusions [org.clojure/clojure]]
                                  [org.clojure/clojure "1.5.1"]
                                  [org.apache.hadoop/hadoop-core "1.1.2"]
                                  [asm "3.2"]
                                  [cascading/cascading-hadoop "[2.2.0,2.3.0)"
                                   :exclusions [org.codehaus.janino/janino
                                                org.apache.hadoop/hadoop-core]]]}}
  :plugins [[lein2-eclipse "2.0.0"]])
