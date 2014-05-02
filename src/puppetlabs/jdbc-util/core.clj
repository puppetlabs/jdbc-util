(ns puppetlabs.jdbc-util
  (:require [clojure.java.jdbc           :as jdbc]
            [puppetlabs.kitchensink.core :as ks])
(defn- convert-result-arrays
  "Converts Java and JDBC arrays in a result set using the provided function
   (eg. vec, set). Values which aren't arrays are unchanged."
  ([result-set]
     (convert-result-arrays vec result-set))
  ([f result-set]
     (let [convert #(cond
                     (ks/array? %) (f %)
                     (isa? (class %) java.sql.Array) (f (.getArray %))
                     :else %)]
       (map #(ks/mapvals convert %) result-set))))

(defn- query
  "An implementation of query that returns a fully evaluated result (no
   JDBCArray objects, etc)"
  [db sql-and-params]
  (let [convert (fn [rs]
                  (doall
                   (convert-result-arrays vec
                                          (jdbc/result-set-seq rs))))]
    (jdbc/db-query-with-resultset db sql-and-params convert)))
