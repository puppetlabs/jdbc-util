(ns puppetlabs.jdbc-util.cp-test
  (:require [clojure.test :refer :all]
            [clojure.java.jdbc :as jdbc]
            [puppetlabs.jdbc-util.cp :refer :all]
            [puppetlabs.jdbc-util.testutils :refer [test-db]]
            [puppetlabs.trapperkeeper.testutils.logging :as logging]))

(deftest connection
  (testing "test that the connection pool works"
    (logging/with-test-logging
      (let [db (pooled-db-spec "jdbc-util-test" test-db)]
        (close-pooled-db-spec db)))))

(deftest operations
  (testing "test basic jdbc operations work"
    (logging/with-test-logging
      (let [db (pooled-db-spec "jdbc-util-test" test-db)]
        (= (jdbc/query db "select 1 as foo")
           {:foo 1})
        (close-pooled-db-spec db)))))

(deftest transactions
  (testing "test changing transaction level"
    (logging/with-test-logging
      (let [db (pooled-db-spec "jdbc-util-test" test-db)]
        (jdbc/with-db-transaction [t db :isolation :serializable]
          (= (jdbc/query db "select 1 as foo")
             {:foo 1}))

        (jdbc/with-db-transaction [t db :isolation :read-committed]
          (= (jdbc/query db "select 1 as foo")
             {:foo 1}))

        (close-pooled-db-spec db)))))
