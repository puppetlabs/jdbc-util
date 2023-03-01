(defproject puppetlabs/jdbc-util "1.4.1-SNAPSHOT"
  :description "Common JDBC helpers for use in Puppet Labs projects"
  :url "https://github.com/puppetlabs/jdbc-util"

  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}

  :pedantic? :abort
  :dependencies [[org.clojure/clojure]
                 [org.clojure/java.jdbc]
                 [org.clojure/test.check "0.9.0"]
                 [org.postgresql/postgresql]
                 [migratus "1.3.5" :exclusions [org.clojure/clojure]]
                 [com.zaxxer/HikariCP "5.0.1"]
                 [puppetlabs/kitchensink]
                 [puppetlabs/i18n]
                 [io.dropwizard.metrics/metrics-core]
                 [io.dropwizard.metrics/metrics-healthchecks "3.2.2"]
                 [cheshire]]

  :profiles {:dev {:dependencies [[org.slf4j/slf4j-api "1.7.25"]
                                  [org.slf4j/slf4j-log4j12 "1.7.25"]
                                  [log4j/log4j "1.2.17"]]}}

  :plugins [[lein-release "1.0.9"]
            [lein-parent "0.3.7"]
            [puppetlabs/i18n "0.9.2" :exclusions [org.clojure/clojure]]]

  :jar-exclusions [#"\.sw[a-z]$" #"~$" #"logback\.xml$" #"log4j\.properties$"]

  :lein-release {:scm :git
                 :deploy-via :lein-deploy}

  :parent-project {:coords [puppetlabs/clj-parent "5.3.0"]
                   :inherit [:managed-dependencies]}

  :deploy-repositories [["releases" {:url "https://clojars.org/repo"
                                     :username :env/clojars_jenkins_username
                                     :password :env/clojars_jenkins_password
                                     :sign-releases false}]])
