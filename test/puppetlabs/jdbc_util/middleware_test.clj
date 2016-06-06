(ns puppetlabs.jdbc-util.middleware-test
  (:import [org.postgresql.util PSQLException PSQLState])
  (:require [cheshire.core :as json]
            [clojure.test :refer :all]
            [puppetlabs.jdbc-util.middleware :refer :all]))

(deftest pg-permission-error-middleware-test
  (let [throwing-handler (fn [_] (throw (PSQLException. "SomeMessage" (PSQLState. "42501"))))
        wrapped (handle-postgres-permission-errors throwing-handler)
        response (wrapped nil)
        response-body (-> response :body (json/parse-string true))]
    (is (= 500 (:status response)))
    (is (= "db-permission-error" (:kind response-body)))))
