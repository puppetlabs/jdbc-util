(ns puppetlabs.jdbc-util.core-test
  (:require [clojure.test :refer :all]
            [clojure.walk :refer [keywordize-keys]]
            [clojure.java.jdbc :as jdbc]
            [puppetlabs.jdbc-util.core :refer :all]))

(def test-db
  {:classname "org.postgresql.Driver"
   :subprotocol "postgresql"
   :subname (or (System/getenv "JDBCUTIL_DBNAME") "jdbc_util_test")
   :user (or (System/getenv "JDBCUTIL_DBUSER") "jdbc_util_test")
   :password (or (System/getenv "JDBCUTIL_DBPASS") "foobar")})

(defn setup-db [db]
  (jdbc/execute! db ["CREATE TABLE authors (
                        name TEXT PRIMARY KEY,
                        favorite_color TEXT)"])
  (jdbc/execute! db ["CREATE TABLE books (
                        title TEXT PRIMARY KEY,
                        author TEXT REFERENCES authors (name))"])
  (jdbc/insert! db :authors
                {:name "kafka"  :favorite_color "black"}
                {:name "borges" :favorite_color "purple"}
                {:name "woolf"  :favorite_color "yellow"}
                {:name "no one" :favorite_color "gray"})
  (jdbc/insert! db :books
                {:title "the castle"                  :author "kafka"}
                {:title "the trial"                   :author "kafka"}
                {:title "the aleph"                   :author "borges"}
                {:title "library of babel"            :author "borges"}
                {:title "the garden of forking paths" :author "borges"}
                {:title "the voyage out"              :author "woolf"}
                {:title "the waves"                   :author "woolf"}))

(use-fixtures :once
              (fn [f]
                (let [env #(System/getenv %)]
                  (drop-public-tables! test-db)
                  (setup-db test-db)
                  (f))))

(defn find-author [rows name]
  (first (filter #(= name (:name %)) rows)))

(deftest db-up?-test
  (testing "db-up?"
    (testing "returns true when the DB is up"
      (is (db-up? test-db)))

    (testing "returns false if the query function throws an exception"
      (with-redefs [jdbc/query (fn [_ _] (throw (Exception. "what DB?")))]
        (is (false? (db-up? nil)))))

    (testing "returns false if the arithmetic doesn't check out"
      (with-redefs [jdbc/query (fn [_ _] [{:answer 49}])]
        (is (false? (db-up? nil)))))

    (testing "returns false if the check times out"
      (with-redefs [jdbc/query (fn [_ _] (Thread/sleep 2000) [{:answer 42}])]
        (is (false? (db-up? nil)))))))

(deftest connection-pool-test
  (testing "connection-pool returns a usable DB spec"
    (let [pooled-db (connection-pool test-db)]
      (is (db-up? pooled-db))

      (testing "pooled connections retry, rather than failing immediately"
        (let [bad-db (assoc test-db :subname "//example.com/xyz")
              bad-pool (-> (connection-pool bad-db)
                         (update-in [:datasource] #(doto % (.setConnectionTimeout 5000))))
              start (System/currentTimeMillis)
              result (try (jdbc/query bad-pool ["dummy"])
                          (catch java.sql.SQLTransientConnectionException _
                            ::timeout))
              end (System/currentTimeMillis)]
          (is (<= 5000 (- end start)))
          (is (= ::timeout result)))))))

(deftest ^:database querying
  (testing "works within a transaction"
    (let [rows (jdbc/with-db-transaction [test-db test-db]
                 (query test-db ["SELECT name FROM authors ORDER BY name LIMIT 1"]))]
      (is (= "borges" (:name (first rows))))))

  (testing "properly translates arrays"
    (let [rows (query test-db ["SELECT a.name, a.favorite_color, array_agg(b.title) AS books
                                  FROM authors a JOIN books b ON a.name = b.author
                                  GROUP BY a.name, a.favorite_color"])
          borges (find-author rows "borges")
          woolf  (find-author rows "woolf")
          kafka  (find-author rows "kafka")]
      (is (= 3 (count rows)))
      (is (every? #(= #{:name :favorite_color :books} (set (keys %))) rows))
      (are [x y] (= x y)
           "borges" (:name borges)
           "woolf"  (:name woolf)
           "kafka"  (:name kafka)
           "purple" (:favorite_color borges)
           "yellow" (:favorite_color woolf)
           "black"  (:favorite_color kafka)
           true     (vector? (:books borges))
           true     (vector? (:books woolf))
           true     (vector? (:books kafka))
           #{"the aleph" "library of babel" "the garden of forking paths"} (set (:books borges))
           #{"the castle" "the trial"}                                     (set (:books kafka))
           #{"the waves" "the voyage out"}                                 (set (:books woolf)))))

  (testing "properly selects when no arrays present"
    (let [rows (query test-db ["SELECT name, favorite_color FROM authors"])
          borges (find-author rows "borges")
          woolf  (find-author rows "woolf")
          kafka  (find-author rows "kafka")
          no-one (find-author rows "no one")]
      (is (= 4 (count rows)))
      (is (every? #(= #{:name :favorite_color} (set (keys %))) rows))
      (are [x y] (= x y)
           "no one" (:name no-one)
           "gray"   (:favorite_color no-one)
           "borges" (:name borges)
           "woolf"  (:name woolf)
           "kafka"  (:name kafka)
           "purple" (:favorite_color borges)
           "yellow" (:favorite_color woolf)
           "black"  (:favorite_color kafka)))))

(deftest convert-results-arrays-when-not-java-array
  (testing "Arrays are processed while non-arrays are passed through"
    (let [rows   [{:one "two" :three (into-array [1 2 3])}]
          result (first (convert-result-arrays rows))]
      (are [x y] (= x y)
           true (vector? (:three result))
           #{1 2 3} (set (:three result))
           "two" (:one result))))

  (testing "can pass a function to override vec default"
    (let [rows   [{:one "two" :three (into-array [1 2 3])}]
          result (first (convert-result-arrays set rows))]
      (is (set? (:three result)))
      (is (= #{1 2 3} (:three result))))))

(deftest submap-aggregation
  (testing "handles nils"
    (is (= '({:a 1 :b 2 :cs {}})
           (->> '({:a 1 :b 2 :c nil :d nil})
                (aggregate-submap-by :c :d :cs)))))

  (testing "nested calls"
    (let [rows [{:value "one-val",
                 :parameter "one",
                 :class "first",
                 :environment "test",
                 :name "group-params"}
                {:value "two-val",
                 :parameter "two",
                 :class "first",
                 :environment "test",
                 :name "group-params"}
                {:value "three-val"
                 :parameter "three"
                 :class "second"
                 :environment "test"
                 :name "group-params"}]]
      (is (= '({:name "group-params"
                :environment "test"
                :classes {:first {:one "one-val"
                                  :two "two-val"}
                          :second {:three "three-val"}}})
             (->> rows
               (aggregate-submap-by :parameter :value :parameters)
               (aggregate-submap-by :class :parameters :classes)
               (keywordize-keys)))))))

(deftest column-aggregation
  (testing "aggregates as expected while preserving order"
    (let [rows [{:white 1 :blue 2 :green 0}
                {:white 1 :blue 4 :green 1}
                {:white 1 :blue 6 :green 2}
                {:white 1 :blue 8 :green 3}
                {:white 1 :blue 10 :green 4}
                {:white 1 :blue 12 :green 5}
                {:white 1 :blue 14 :green 6}
                {:white 1 :blue 16 :green 7}
                {:white 1 :blue 18 :green 8}
                {:white 1 :blue 7 :green 2}
                {:white 1 :blue 19 :green 8}
                {:white 1 :blue 13 :green 5}
                {:white 1 :blue 15 :green 6}
                {:white 1 :blue 5 :green 1}
                {:white 1 :blue 11 :green 4}
                {:white 1 :blue 3 :green 0}
                {:white 1 :blue 17 :green 7}
                {:white 1 :blue 9 :green 3}]]
      (is (= [{:white 1 :blues [2 3] :green 0}
              {:white 1 :blues [4 5] :green 1}
              {:white 1 :blues [6 7] :green 2}
              {:white 1 :blues [8 9] :green 3}
              {:white 1 :blues [10 11] :green 4}
              {:white 1 :blues [12 13] :green 5}
              {:white 1 :blues [14 15] :green 6}
              {:white 1 :blues [16 17] :green 7}
              {:white 1 :blues [18 19] :green 8}]
             (aggregate-column :blue :blues rows))))))

(deftest seq-param-expansion
  (testing "expand-seq-params expands sequential parameters with appropriate
           placeholder while ignoring other parameters"
    (is (= ["SELECT * FROM kerjiggers WHERE version = ? AND name IN (?,?,?) AND manufacturer IN (?,?,?,?)"
            "2.0.0"
            "doohickey" "jobby" "thingie"
            "US Robotics" "Omnicorp" "Cyberdyne Systems" "Morgan Industries"]
           (expand-seq-params
             ["SELECT * FROM kerjiggers WHERE version = ? AND name IN ? AND manufacturer IN ?"
              "2.0.0"
              ["doohickey" "jobby" "thingie"]
              ["US Robotics" "Omnicorp" "Cyberdyne Systems" "Morgan Industries"]])))))
