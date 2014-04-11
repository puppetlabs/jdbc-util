(defproject puppetlabs.jdbc-util "0.1.0-SNAPSHOT"
  :description "Common JDBC helpers for use in Puppet Labs projects"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/java.jdbc "0.3.0"]
                 [org.postgresql/postgresql "9.3-1100-jdbc41"]
                 [com.jolbox/bonecp "0.8.0.RELEASE" :exclusions [[org.slf4j/slf4j-api]]]])
