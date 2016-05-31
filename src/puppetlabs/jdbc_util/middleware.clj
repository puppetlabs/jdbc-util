(ns puppetlabs.jdbc-util.middleware
  (:import [org.postgresql.util PSQLException])
  (:require [cheshire.core :as json]
            [puppetlabs.i18n.core :refer [tru]]))

(defn handle-postgres-permission-errors
  "A ring middleware to catch and report postgresql permission-denied errors."
  [handler]
  (fn [request]
    (try
      (handler request)
      (catch PSQLException e
        (if (= "42501" (.getSQLState e))
          {:status 403
           :headers {"Content-Type" "application/json"}
           :body (json/generate-string {:kind "db-permission-error"
                                        :msg (tru "The operation could not be performed because of insufficient database permissions.")})}
          (throw e))))))
