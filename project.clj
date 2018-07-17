(defproject puppetlabs/jdbc-util "1.2.2-SNAPSHOT"
  :description "Common JDBC helpers for use in Puppet Labs projects"
  :url "https://github.com/puppetlabs/jdbc-util"

  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}

  :pedantic? :abort
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/java.jdbc "0.7.7"]
                 [org.clojure/test.check "0.9.0"]
                 [org.postgresql/postgresql "42.2.0"]
                 [migratus "1.0.8" :exclusions [org.clojure/clojure]]
                 [com.zaxxer/HikariCP "2.7.4"]
                 [puppetlabs/kitchensink "2.3.0"]
                 [puppetlabs/i18n "0.8.0"]
                 [io.dropwizard.metrics/metrics-core "3.1.2"]
                 [io.dropwizard.metrics/metrics-healthchecks "3.1.2"]
                 [cheshire "5.7.1"]]

  :profiles {:dev {:dependencies [[org.slf4j/slf4j-api "1.7.25"]
                                  [org.slf4j/slf4j-log4j12 "1.7.25"]
                                  [log4j/log4j "1.2.17"]]}}

  :plugins [[lein-release "1.0.5"]
            ;; pin clojure to resolve dependency conflict with lein-release and i18n.
            [org.clojure/clojure "1.8.0"]
            [puppetlabs/i18n "0.8.0"]]

  :jar-exclusions [#"\.sw[a-z]$" #"~$" #"logback\.xml$" #"log4j\.properties$"]

  :lein-release {:scm :git
                 :deploy-via :lein-deploy}

  :deploy-repositories [["releases" {:url "https://clojars.org/repo"
                                     :username :env/clojars_jenkins_username
                                     :password :env/clojars_jenkins_password
                                     :sign-releases false}]])
