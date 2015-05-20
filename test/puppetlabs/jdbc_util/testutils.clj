(ns puppetlabs.jdbc-util.testutils)

(def test-db {:classname "org.postgresql.Driver"
              :subprotocol "postgresql"
              :subname (or (System/getenv "TEST_DBSUBNAME") "jdbc_util_test")
              :user (or (System/getenv "TEST_DBUSER") "jdbc_util_test")
              :password (or (System/getenv "TEST_DBPASS") "foobar")})
