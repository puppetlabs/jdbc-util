(ns puppetlabs.jdbc-util.pool-test
  (:require [clojure.test :refer :all]
            [puppetlabs.jdbc-util.core :as core]
            [puppetlabs.jdbc-util.core-test :as core-test]
            [puppetlabs.jdbc-util.pool :as pool])
  (:import com.zaxxer.hikari.HikariConfig
           java.io.Closeable
           [java.sql Connection SQLTransientConnectionException]
           javax.sql.DataSource))

(deftest pool-creation
  (let [test-pool (-> core-test/test-db
                      pool/spec->hikari-options
                      pool/options->hikari-config
                      pool/connection-pool)]
    (testing "can create a connection pool from a db spec"
      (is (core/db-up? test-pool)))
    (.close (:datasource test-pool))))

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
  (let [config (pool/options->hikari-config
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
                 ;; :metric-registry
                 ;; :health-check-registry
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
        "bob" .getPoolName))

    (let [config (pool/options->hikari-config
                  {:username "nobody"
                   :password "noone"
                   :data-source-class-name "org.postgresql.ds.PGSimpleDataSource"})]
      (testing "can set data-source-class-name"
        (is (= "org.postgresql.ds.PGSimpleDataSource"
               (.getDataSourceClassName config)))))))

(deftest delayed-init
  (let [ready (atom false)
        retries (atom 0)
        mock-connection (reify
                          Connection
                          Closeable
                          (close [_] nil))
        mock-ds (reify
                  DataSource
                  (getConnection [_]
                    (if @ready
                      mock-connection
                      (do
                        (swap! retries inc)
                        (Thread/sleep 500)
                        (throw (SQLTransientConnectionException. "oops")))))
                  (getConnection [this user pass]
                    (.getConnection this)))]
    (testing "the pool retries until it gets a connection"
      (let [wrapped (pool/wrap-with-delayed-init mock-ds (fn [_] mock-ds) 1)]
        (is (thrown? SQLTransientConnectionException
                     (.getConnection (:datasource wrapped))))
        (Thread/sleep 2000)
        (swap! ready (constantly true))
        (Thread/sleep 600)
        (.getConnection (:datasource wrapped))
        (is (<= 4 @retries 5))))  ; allow for some variance due to timing

    (testing "if the init-fn throws an exception it doesn't give out connections"
      (swap! ready (constantly true))  ; don't depend on state from the previous test
      (let [wrapped (pool/wrap-with-delayed-init
                     mock-ds (fn [_] (throw (RuntimeException.))) 1)]
        (is (thrown? java.util.concurrent.ExecutionException
                     (.getConnection (:datasource wrapped))))))))
