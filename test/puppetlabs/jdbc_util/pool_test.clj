(ns puppetlabs.jdbc-util.pool-test
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.test :refer :all]
            [puppetlabs.i18n.core :refer [with-user-locale string-as-locale]]
            [puppetlabs.jdbc-util.core :as core]
            [puppetlabs.jdbc-util.core-test :as core-test]
            [puppetlabs.jdbc-util.migration :as migration]
            [puppetlabs.jdbc-util.pool :as pool])
  (:import com.codahale.metrics.health.HealthCheckRegistry
           [com.zaxxer.hikari HikariConfig HikariDataSource]
           java.io.Closeable
           [java.sql Connection SQLTransientConnectionException]
           javax.sql.DataSource))

(deftest spec->hikari-config
  (let [spec {:subprotocol "postgresql"
              :subname "jdbc_util_test?ssl=true&something=false"
              :user "jdbc_util_user"
              :password "frog"}
        options (pool/spec->hikari-options spec)
        config (pool/options->hikari-config options)]

    (testing "can create a valid configuration"
      (is (instance? HikariConfig config))
      (.validate config))

    (testing "the passed in values are correct"
      (is (= "jdbc_util_user" (.getUsername config)))
      (is (= "jdbc:postgresql:jdbc_util_test?ssl=true&something=false"
             (.getJdbcUrl config)))
      (is (= "frog" (.getPassword config))))))

(deftest options->hikari-config
  (let [health-reg (HealthCheckRegistry.)
        config (pool/options->hikari-config
                {:username "trogdor"
                 :password "burn1n4t3"
                 :driver-class-name "org.postgresql.Driver"
                 :jdbc-url "jdbc:postgresql:jdbc_util_test"
                 :auto-commit false
                 :connection-timeout 1200
                 :idle-timeout 20002
                 :max-lifetime 32001
                 :connection-test-query "SELECT 1 FROM ANYWHERE;"
                 :minimum-idle 5
                 :maximum-pool-size 200
                 :health-check-registry health-reg
                 :connection-check-timeout 666
                 ;; :metric-registry
                 :pool-name "bob"})]
    (testing "can set options on a HikariConfig"
      (are [value getter] (= value (getter config))
        "trogdor" .getUsername
        "burn1n4t3" .getPassword
        "org.postgresql.Driver" .getDriverClassName
        "jdbc:postgresql:jdbc_util_test" .getJdbcUrl
        false .isAutoCommit
        1200 .getConnectionTimeout
        20002 .getIdleTimeout
        32001 .getMaxLifetime
        "SELECT 1 FROM ANYWHERE;" .getConnectionTestQuery
        5 .getMinimumIdle
        200 .getMaximumPoolSize
        "bob" .getPoolName
        health-reg .getHealthCheckRegistry
        {"connectivityCheckTimeoutMs" "666"} .getHealthCheckProperties)))

  (testing "when no health registry is set, the health check timeout can still be set"
    (let [config (pool/options->hikari-config {:driver-class-name "org.postgresql.Driver"
                                               :jdbc-url "jdbc:postgresql:jdbc_util_test"
                                               :connection-check-timeout 666})]
      (is (= {"connectivityCheckTimeoutMs" "666"} (.getHealthCheckProperties config)))

      (testing "and persists after a health registry is set"
        (.setHealthCheckRegistry config (HealthCheckRegistry.))
        (is (= {"connectivityCheckTimeoutMs" "666"} (.getHealthCheckProperties config))))))

  (let [config (pool/options->hikari-config
                 {:username "nobody"
                  :password "noone"
                  :data-source-class-name "org.postgresql.ds.PGSimpleDataSource"})]
    (testing "can set data-source-class-name"
      (is (= "org.postgresql.ds.PGSimpleDataSource"
             (.getDataSourceClassName config))))))

(deftest delayed-init
  (let [ready (atom false)
        retries (atom 0)
        mock-connection (reify
                          Connection
                          Closeable
                          (close [_] nil))
        health-registry (atom nil)
        mock-ds (proxy [HikariDataSource] []
                  (getConnection
                    ([]
                     (if @ready
                       mock-connection
                       (do
                         (swap! retries inc)
                         (Thread/sleep 500)
                         (throw (SQLTransientConnectionException. "oops")))))
                    ([user pass]
                     (.getConnection this))))]
    (testing "the pool retries until it gets a connection"
      (let [wrapped (pool/wrap-with-delayed-init mock-ds (fn [_] mock-ds) 1)]
        (is (thrown? SQLTransientConnectionException
                     (.getConnection wrapped)))
        (Thread/sleep 2000)
        (is (= {:state :starting}
               (pool/status wrapped)))
        (swap! ready (constantly true))
        (Thread/sleep 600)
        (.getConnection wrapped)
        ;; allow for some variance due to timing
        (is (<= 4 @retries 5))))))

(deftest delayed-init-real-db
  (let [config (-> core-test/test-db
                   pool/spec->hikari-options
                   pool/options->hikari-config)]
    (testing "if the init-fn throws an exception it continues to hand out connections normally"
      (let [wrapped (pool/connection-pool-with-delayed-init
                     config (fn [_] (throw (RuntimeException. "test exception"))) 10000)]
        (is (= [{:a 1}] (jdbc/query {:datasource wrapped} ["select 1 as a"])))
        (is (= {:state :error
                :messages ["Initialization resulted in an error: test exception"]}
               (pool/status wrapped)))))

    (testing "if init-fn throws an exception in a non english locale"
      (let [eo (string-as-locale "eo")]
        (with-user-locale eo
         (let [wrapped (pool/connection-pool-with-delayed-init
                        config (fn [_] (throw (RuntimeException. "test exception"))) 10000)]
                (is (= [{:a 1}] (jdbc/query {:datasource wrapped} ["select 1 as a"])))
                (is (= {:state :error
                        :messages ["This_is_a_translated_string: test exception"]}
                       (pool/status wrapped)))))))))

(deftest health-check
  (let [test-pool (-> core-test/test-db
                      pool/spec->hikari-options
                      pool/options->hikari-config
                      (pool/connection-pool-with-delayed-init identity 5000))]
    (.getConnection test-pool)
    (is (= {:state :ready}
           (pool/status test-pool)))
    (.close test-pool)))

(deftest migration-db-spec-test
  (let [datasource (-> core-test/test-db
                       (assoc :pool-name "AppPool")
                       pool/spec->hikari-options
                       pool/options->hikari-config)
        migration-db-spec (assoc core-test/test-db :user "migrator")]
    (testing "the init function is called with the application datasource"
      (let [!init-with (promise)
            init-fn (fn [init-with]
                      (deliver !init-with init-with))]
        (pool/connection-pool-with-delayed-init datasource {:migration-db migration-db-spec} init-fn 5000)
        (let [init-with (deref !init-with 5000 nil)]
          (is (= "AppPool" (.getPoolName (:datasource init-with)))))))
    (testing "the migrate function is called with the migration-db-spec"
      (let [!migrate-with (promise)]
        (with-redefs [migration/migrate (fn [migrate-db migration-dir]
                                          (deliver !migrate-with migrate-db))]
          (pool/connection-pool-with-delayed-init datasource {:migration-db migration-db-spec} identity 5000)
          (let [migrate-with (deref !migrate-with 5000 nil)]
            (is (= migration-db-spec migrate-with))))))))
