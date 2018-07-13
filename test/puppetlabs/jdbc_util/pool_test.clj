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
           (java.util.concurrent CancellationException)))

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
      (with-open [wrapped (pool/wrap-with-delayed-init mock-ds (fn [_] mock-ds) 1)]
        (is (thrown? SQLTransientConnectionException
                     (.getConnection wrapped)))
        (Thread/sleep 2000)
        (is (= {:state :starting}
               (pool/status wrapped)))
        (reset! ready true)
        (Thread/sleep 600)
        (.getConnection wrapped)
        ;; allow for some variance due to timing
        (is (<= 4 @retries 5))))))

(deftest delayed-lifecyle
  (let [config (-> core-test/test-db
                   pool/spec->hikari-options
                   pool/options->hikari-config)]
    (testing "block-until-ready blocks until it is ready"
      (core/drop-public-tables! core-test/test-db)
      (let [ready (promise)]
        (with-open [wrapped (pool/connection-pool-with-delayed-init
                              config (fn [_] (deref ready)) 10000)]
          (is (= false (pool/init-complete? wrapped)))
          (is (= false (pool/block-until-ready wrapped 100)))
          (deliver ready true)
          (is (pool/block-until-ready wrapped 100))
          (is (pool/init-complete? wrapped)))))

    (testing "block-until-ready blocks not impacted by exception"
      (core/drop-public-tables! core-test/test-db)
      (let [ready (promise)]
        (with-open [wrapped (pool/connection-pool-with-delayed-init
                              config
                              (fn [_]
                                (deref ready)
                                (throw (RuntimeException. "test exception")))
                              10000)]
          (is (= false (pool/init-complete? wrapped)))
          (is (= false (pool/block-until-ready wrapped 500)))
          (deliver ready true)
          (is (pool/block-until-ready wrapped 500))
          (is (pool/init-complete? wrapped))
          (is (thrown? RuntimeException (.getConnection wrapped))))))

    (testing "cancel-init cancels the init"
      (core/drop-public-tables! core-test/test-db)
      (let [ready (promise)]
        (with-open [wrapped (pool/connection-pool-with-delayed-init
                              config (fn [_] (deref ready)) 10000)]
          (is (= false (pool/init-complete? wrapped)))
          (is (pool/cancel-init wrapped))
          ;; note -- complete but cancelled!
          (is (pool/init-complete? wrapped)))))

    (testing "cancel init when init already complete"
      (core/drop-public-tables! core-test/test-db)
      (let [ready (promise)]
        (with-open [wrapped (pool/connection-pool-with-delayed-init
                              config (fn [_] (deref ready)) 10000)]
          (is (= false (pool/init-complete? wrapped)))
          (deliver ready true)
          (is (pool/block-until-ready wrapped 500))
          (is (pool/init-complete? wrapped))
          (is (= false (pool/cancel-init wrapped))))))

    (testing "exception thrown during init-fn does not impact shutdown"
      (core/drop-public-tables! core-test/test-db)
      (let [ready (promise)]
        (with-open [wrapped (pool/connection-pool-with-delayed-init
                              config
                              (fn [_]
                                (deref ready)
                                (throw (Exception. "throw exception during init")))
                              10000)]
          (is (= false (pool/init-complete? wrapped)))
          (deliver ready true)
          (is (pool/block-until-ready wrapped 500))
          (is (pool/init-complete? wrapped))
          (is (= false (pool/cancel-init wrapped))))))

    (testing "exception thrown during replica wait for migration does not impact shutdown"
      (core/drop-public-tables! core-test/test-db)

      (let [ready (promise)
            willing (promise)]
        (with-redefs [pool/wait-for-migrations (fn [_ _]
                                                 (deliver willing true)
                                                 (deref ready)
                                                 (throw (Exception. "throw exception during migration waiting")))]
          (with-open [wrapped (pool/connection-pool-with-delayed-init
                                config
                                {:migration-db     core-test/test-db
                                 :migration-dir    "test-migrations"
                                 :replication-mode :replica}
                                (fn [_] (deref ready))
                                10000)]
            (deref willing)
            (is (= false (pool/init-complete? wrapped)))
            (deliver ready true)
            (is (pool/block-until-ready wrapped 500))
            (is (thrown? RuntimeException (.getConnection wrapped)))
            (is (pool/init-complete? wrapped))
            (is (= false (pool/cancel-init wrapped)))))))))

(deftest delayed-init-real-db
  (core/drop-public-tables! core-test/test-db)

  (let [config (-> core-test/test-db
                   pool/spec->hikari-options
                   pool/options->hikari-config)]
    (testing "if the init-fn throws an exception it doesn't throw exceptions"
      (with-open [wrapped (pool/connection-pool-with-delayed-init
                            config (fn [_] (throw (RuntimeException. "test exception"))) 10000)]
        (is (= [{:a 1}] (jdbc/query {:datasource wrapped} ["select 1 as a"])))
        (is (thrown? RuntimeException (jdbc/query wrapped ["select 1 as a"])))
        (is (= {:state :error
                :messages ["Initialization resulted in an error: test exception"]}
               (pool/status wrapped)))))

    (testing "if init-fn throws an exception in a non english locale"
      (let [fo (string-as-locale "fo")]
        (with-user-locale fo
         (let [wrapped (pool/connection-pool-with-delayed-init
                        config (fn [_] (throw (RuntimeException. "test exception"))) 10000)]
           (is (= [{:a 1}] (jdbc/query {:datasource wrapped} ["select 1 as a"])))
           (is (= {:state :error
                   :messages ["This_is_a_translated_string: test exception"]}
                  (pool/status wrapped)))))))))

(deftest health-check
  (core/drop-public-tables! core-test/test-db)

  (with-open [test-pool (-> core-test/test-db
                            pool/spec->hikari-options
                            pool/options->hikari-config
                            (pool/connection-pool-with-delayed-init identity 5000))]
    (.getConnection test-pool)
    (is (= {:state :ready}
           (pool/status test-pool)))))

(deftest migration-db-spec-test
  (core/drop-public-tables! core-test/test-db)

  (let [datasource (-> core-test/test-db
                       (assoc :pool-name "AppPool")
                       pool/spec->hikari-options
                       pool/options->hikari-config)
        migration-db-spec (assoc core-test/test-db :user "migrator")]
    (testing "the init function is called with the application datasource"
      (let [!init-with (promise)
            init-fn (fn [init-with]
                      (deliver !init-with init-with))]
        (let [wrapped (pool/connection-pool-with-delayed-init
                        datasource {:migration-db migration-db-spec} init-fn 5000)]
          (let [init-with (deref !init-with 5000 nil)]
            (is (= "AppPool" (.getPoolName (:datasource init-with)))))
          (pool/close-after-ready wrapped))))

    (testing "the migrate function is called with the migration-db-spec"
      (let [!migrate-with (promise)]
        (with-redefs [migration/migrate (fn [migrate-db migration-dir]
                                          (deliver !migrate-with migrate-db))]
          (let [wrapped (pool/connection-pool-with-delayed-init
                          datasource {:migration-db migration-db-spec} identity 5000)]
            (let [migrate-with (deref !migrate-with 5000 nil)]
             (is (= migration-db-spec migrate-with)))
            (pool/close-after-ready wrapped)
            (core/drop-public-tables! migration-db-spec)))))))

(deftest migration-blocks-initialization
  (let [config (-> core-test/test-db
                   (assoc :pool-name "AppPool")
                   pool/spec->hikari-options
                   pool/options->hikari-config)
        ;; note, this function has side-effects and creates the migration table
        num-migrations-left (fn []
                              (count (migration/uncompleted-migrations core-test/test-db
                                                                       "test-migrations")))
        init-status (atom :waiting)
        wait-for-migrations-ms 3500]
    (core/drop-public-tables! core-test/test-db)
    ;; NOTE - if this fails with an odd batch exception about not being able to create the table it is
    ;; because another test created the schema migration as a user that the test user can't see.  Drop
    ;; the table manually and try again.
    (is (= 1 (num-migrations-left)))
    (testing "waiting on migrations blocks startup on replicas"
      (with-open [wrapped (pool/connection-pool-with-delayed-init
                            config
                            {:migration-db     core-test/test-db
                             :migration-dir    "test-migrations"
                             :replication-mode :replica}
                            (fn [_] (reset! init-status :completed))
                            5000)]
        (is (= 1 (num-migrations-left)))
        (is (= :waiting @init-status))

        (migration/migrate core-test/test-db "test-migrations")
        (pool/block-until-ready wrapped wait-for-migrations-ms)

        (is (= 0 (num-migrations-left)))
        (is (= :completed @init-status))))))

(deftest connection-pool-fails-slow
  (testing "given a configuration for a non-existent database"
    (let [options (pool/spec->hikari-options {:user "fakeuser"
                                              :password "fakepassword"
                                              :subprotocol "postgresql"
                                              :subname "nocalhost"
                                              :connection-timeout 1000})
          config (pool/options->hikari-config options)]
      (testing "calling connection-pool-with-delayed-init doesn't throw an exception"
        (let [wrapped (pool/connection-pool-with-delayed-init config
                                                              {}
                                                              nil
                                                              1000)]
          (pool/cancel-init wrapped)
          (is (thrown? CancellationException (pool/close-after-ready wrapped))))))))
