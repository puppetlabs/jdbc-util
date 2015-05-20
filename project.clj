(def ks-version "1.1.0")

(defproject puppetlabs/jdbc-util "0.1.1-SNAPSHOT"
  :description "Common JDBC helpers for use in Puppet Labs projects"
  :url "https://github.com/puppetlabs/jdbc-util"

  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}

  :dependencies [[com.jolbox/bonecp "0.8.0.RELEASE" :exclusions [[org.slf4j/slf4j-api]]]
                 [com.zaxxer/HikariCP-java6 "2.3.7" :exclusions [[org.slf4j/slf4j-api]]]
                 [metrics-clojure "2.5.1" :exclusions [[org.slf4j/slf4j-api]]]
                 [org.clojure/clojure "1.6.0"]
                 [org.clojure/java.jdbc "0.3.6"]
                 [org.postgresql/postgresql "9.4-1201-jdbc41"]
                 [prismatic/schema "0.4.2"]
                 [puppetlabs/kitchensink ~ks-version]]

  :profiles {:dev {:dependencies [[puppetlabs/trapperkeeper "1.1.1"]
                                  [puppetlabs/trapperkeeper "1.1.1" :classifier "test"]]}}

  :source-paths ["src/clj"]
  :java-source-paths ["src/java"]

  :plugins [[lein-release "1.0.5"]]

  :lein-release {:scm :git
                 :deploy-via :lein-deploy}

  :deploy-repositories [["releases" {:url "https://clojars.org/repo"
                                     :username :env/clojars_jenkins_username
                                     :password :env/clojars_jenkins_password
                                     :sign-releases false}]]
)
