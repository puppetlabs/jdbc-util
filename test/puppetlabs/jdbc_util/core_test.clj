(ns puppetlabs.jdbc-util.core-test
  (:import [java.util UUID]
           [org.postgresql.util PSQLException PSQLState PGobject])
  (:require [cheshire.core :as json]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as tc-gen]
            [clojure.test.check.properties :as tc-prop]
            [clojure.walk :refer [keywordize-keys]]
            [puppetlabs.jdbc-util.core :refer :all]
            [puppetlabs.kitchensink.core :as ks]))

(def test-db
  {:subprotocol "postgresql"
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
  (jdbc/execute! db ["CREATE TABLE weird_junk ( id UUID PRIMARY KEY )"])
  (jdbc/execute! db ["CREATE TABLE jsonb_testing (
                        name TEXT PRIMARY KEY,
                        text_stuff TEXT,
                        json_stuff JSON,
                        jsonb_stuff JSONB)"])
  (jdbc/insert-multi! db :jsonb_testing
                      [{:name "a"
                        :text_stuff "text"
                        :json_stuff (doto (PGobject.)
                                      (.setType "json")
                                      (.setValue (json/generate-string {:should "return encoded"})))
                        :jsonb_stuff (doto (PGobject.)
                                      (.setType "jsonb")
                                      (.setValue (json/generate-string {:should "return parsed"})))}
                       {:name "b"
                        :text_stuff "text"
                        :json_stuff (doto (PGobject.)
                                      (.setType "json")
                                      (.setValue (json/generate-string {:should "return encoded"})))
                        :jsonb_stuff (doto (PGobject.)
                                      (.setType "jsonb")
                                      (.setValue (json/generate-string {:should "return parsed"})))}])
  (jdbc/insert-multi! db :authors
                      [{:name "kafka"  :favorite_color "black"}
                       {:name "borges" :favorite_color "purple"}
                       {:name "woolf"  :favorite_color "yellow"}
                       {:name "no one" :favorite_color "gray"}])
  (jdbc/insert-multi! db :books
                      [{:title "the castle"                  :author "kafka"}
                       {:title "the trial"                   :author "kafka"}
                       {:title "the aleph"                   :author "borges"}
                       {:title "library of babel"            :author "borges"}
                       {:title "the garden of forking paths" :author "borges"}
                       {:title "the voyage out"              :author "woolf"}
                       {:title "the waves"                   :author "woolf"}])
  (jdbc/insert! db :weird_junk {:id (UUID/randomUUID)}))

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
      (with-redefs [jdbc/query (fn [_ _] (Thread/sleep 5000) [{:answer 42}])]
        (is (false? (db-up? nil)))))))

(defn- subname->db-name
  [subname]
  (-> (java.net.URI. subname)
    .getPath
    (str/replace #"^\/" "")))

(deftest db-exists?-test
  (is (true? (db-exists? test-db (-> test-db :subname subname->db-name))))
  (is (false? (db-exists? test-db "no-database-here"))))

(deftest table-exists?-test
  (is (true? (table-exists? test-db "authors")))
  (is (true? (table-exists? test-db "books")))
  (is (true? (table-exists? test-db "weird_junk")))
  (is (true? (table-exists? test-db "jsonb_testing")))
  (is (false? (table-exists? test-db "no_table_here"))))

(defn- rand-db-name [] (str "jdbc-util-test-db-" (ks/rand-str :alpha-lower 12)))
(defn- rand-username [] (str "jdbc-util-test-user-" (ks/rand-str :alpha-lower 12)))

(deftest has-role?-test
  (testing "has-role?"
    (let [penny (rand-username)
          walker (rand-username) ; probably should be 'bono' huh
          alice (rand-username)]
      (create-user! test-db penny "hunter2")
      (create-user! test-db walker "hunter2")

      (testing "returns false when the user does not have the role"
        (is (false? (has-role? test-db penny walker))))

      (testing "returns true"
        (testing "when the user does have the role"
          (jdbc/execute! test-db [(str "GRANT \"" walker "\" TO \"" penny "\"")])
          (is (true? (has-role? test-db penny walker))))

        (testing "when the user and the role are the same and the user does exist"
          (create-user! test-db alice "hunter2")
          (is (true? (has-role? test-db alice alice)))))

      (doseq [user [penny walker alice]]
        (drop-user! test-db user)))))

(deftest create-db!-test
  (let [test-with-names (fn [db user]
                           (is (false? (db-exists? test-db db)))
                           (create-db! test-db db user)
                           (is (true? (db-exists? test-db db)))
                           (jdbc/execute! test-db
                                          (format "DROP DATABASE %s"
                                                  (pg-escape-identifier db))
                                          {:transaction? false}))]

    (testing "create-db!"
      (testing "works in the simple case"
        (test-with-names (rand-db-name) (:user test-db)))

      (testing "works when creating a database for another role"
        (let [other-user (rand-username)
              rand-db (rand-db-name)]
          (create-user! test-db other-user "hunter2")
          (is (false? (db-exists? test-db rand-db)))
          (create-db! test-db rand-db other-user)
          (is (true? (db-exists? test-db rand-db)))
          (jdbc/execute! test-db
                         (format (str "GRANT %s TO %s"
                                      ";DROP DATABASE %s")
                                 (pg-escape-identifier other-user)
                                 (pg-escape-identifier (:user test-db))
                                 (pg-escape-identifier rand-db))
                         {:transaction? false})
          (drop-user! test-db other-user)))

      (testing "handles DB & user names that try to break quoting"
        (let [bad-db-name "important\";"
              bad-user-name "Robert'\"); DROP TABLE STUDENTS;--"]
          (test-with-names bad-db-name (:user test-db))

          (create-user! test-db bad-user-name "foo")
          (jdbc/execute! test-db
                         [(format "GRANT %s TO %s"
                                  (pg-escape-identifier bad-user-name)
                                  (pg-escape-identifier (:user test-db)))]
                         {:transaction? false})
          (test-with-names (rand-db-name) bad-user-name)
          (drop-user! test-db bad-user-name)))

      (testing "throws an error when given a db that already exists"
        (let [rand-db (rand-db-name)]
          (create-db! test-db rand-db (:user test-db))
          (is (thrown-with-msg? PSQLException #"already exists"
                                (create-db! test-db rand-db (:user test-db))))
          (jdbc/execute! test-db [(format "DROP DATABASE \"%s\"" rand-db)]
                         {:transaction? false}))))))

(deftest drop-db!-test
  (let [test-with-name (fn [db]
                         (jdbc/execute! test-db
                                        [(format "CREATE DATABASE %s" (pg-escape-identifier db))]
                                        {:transaction? false})
                         (is (true? (db-exists? test-db db)))
                         (is (nil? (drop-db! test-db db)))
                         (is (false? (db-exists? test-db db))))]

    (testing "drop-db!"
      (testing "works in the simple case"
        (test-with-name (rand-db-name)))

      (testing "handles db names that try to break quoting"
        (test-with-name "lemme\"out';"))

      (testing "doesn't throw when given a database that doesn't exist"
        (is (nil? (drop-db! test-db "no-such-database")))))))

(deftest user-exists?-test
  (is (true? (user-exists? test-db (:user test-db))))
  (is (false? (user-exists? test-db "Waldo"))))

(deftest create-user!-test
  (let [test-with-name (fn [user]
                         (is (false? (user-exists? test-db user)))
                         (create-user! test-db user "badpassword")
                         (is (true? (user-exists? test-db user)))
                         (jdbc/execute! test-db
                                        [(format "DROP USER %s" (pg-escape-identifier user))]))]

    (testing "create-user!"
      (testing "works in the simple case"
        (test-with-name (rand-username)))

      (testing "works when given names that attempt to break quoting"
        (test-with-name "guccifer\""))

      (testing "throws an error when given a user that already exists"
        (let [rand-user (rand-username)]
          (create-user! test-db rand-user "123")
          (is (thrown-with-msg? PSQLException #"already exists"
                                (create-user! test-db rand-user "123")))
          (jdbc/execute! test-db [(format "DROP USER \"%s\"" rand-user)])))

      (testing "doesn't throw when given a password containing sql"
        ;; I'd like to make a stronger test but typically postgres is configured
        ;; to trust all local connections even if the password doesn't match. It
        ;; would be caught by CI, but then we'd have a test only actually tests
        ;; things when run in CI and not when developers run it
        (let [rand-user (rand-username)]
          (is (nil? (create-user! test-db (rand-username) "';$$; DROP TABLE users"))))))))

(deftest drop-user!-test
  (let [test-with-name (fn [user]
                         (jdbc/execute! test-db
                                        [(format "CREATE USER %s" (pg-escape-identifier user))]
                                        {:transaction? false})
                         (is (true? (user-exists? test-db user)))
                         (drop-user! test-db user)
                         (is (false? (user-exists? test-db user))))]

    (testing "drop-user!"
      (testing "works in the simple case"
        (test-with-name (rand-username)))

      (testing "works with names that try to break quoting"
        (test-with-name "equation'\"group"))

      (testing "doesn't throw when given a user that doesn't exist"
        (is (nil? (drop-user! test-db (rand-username))))))))

(deftest connection-pool-test
  (testing "connection-pool returns a usable DB spec"
    (let [pooled-db (connection-pool test-db)]
      (is (db-up? pooled-db))

      (testing "pooled connections retry, rather than failing immediately"
        (let [bad-db (assoc test-db :subname "//example.puppetlabs/xyz")
              bad-pool (-> (connection-pool bad-db)
                         (update-in [:datasource] #(doto % (.setConnectionTimeout 5000))))
              start (System/currentTimeMillis)
              result (try (jdbc/query bad-pool ["dummy"])
                          (catch java.sql.SQLTransientConnectionException _
                            ::timeout))
              end (System/currentTimeMillis)]
          ;; if we don't explicitly close this, it will keep trying.
          ;; this test generates "connection timeout" messages, and that is normal
          (.close (:datasource bad-pool))

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

(deftest convert-result-pgobjects-test
  (testing "PGobjects are converted to their values"
    (let [[row] (query test-db ["SELECT * FROM weird_junk"])]
      (is (instance? UUID (:id row))))))

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

(deftest has-extension-test
  (testing "look for db extension that exists"
    (is (has-extension? test-db "plpgsql")))

  (testing "look for db extension that does not exist"
    (is (not (has-extension? test-db "notanextension")))))

(deftest quoted-test
  (are [input expected] (= expected (quoted input))
    "a-z"           "\"a-z\""
    "a-z.z-a"       "\"a-z\".\"z-a\""
    "public.foobar" "\"public\".\"foobar\""
    "foobar"        "\"foobar\""
    "a.b.c"         "\"a\".\"b\".\"c\""))

(deftest reconcile-sequence-for-column-test
  (let [max-id #(-> (jdbc/query test-db ["SELECT id FROM sequence_test ORDER BY id DESC LIMIT 1"])
                    first
                    :id)
        insert-dummy #(jdbc/execute! test-db ["INSERT INTO sequence_test DEFAULT VALUES"])
        set-sequence-value #(jdbc/query test-db ["SELECT setval('sequence_test_id_seq', ?)" %])
        current-sequence-value #(-> (jdbc/query test-db ["SELECT last_value FROM sequence_test_id_seq"])
                                    first
                                    :last_value)]
    (testing "given a table with a sequence object"
      (jdbc/execute! test-db ["DROP TABLE IF EXISTS sequence_test CASCADE"])
      (jdbc/execute! test-db ["CREATE TABLE sequence_test (id BIGSERIAL PRIMARY KEY)"])

      (testing "when the table is empty, reconcile-sequence-for-column! sets the sequence to 1"
        (reconcile-sequence-for-column! test-db "sequence_test" "id")
        (insert-dummy)
        (is (= 1 (max-id))))

      (testing "where the sequence object is out of date"
        (dotimes [_ 5] (insert-dummy))
        (set-sequence-value 1)
        (testing "inserting fails"
          (is (thrown-with-msg? PSQLException #"duplicate key value"
                                (insert-dummy)))
          (testing "but not if we call reconcile-sequence-for-column!"
            (set-sequence-value 1)
            (reconcile-sequence-for-column! test-db "sequence_test" "id")
            (insert-dummy))))

      (testing "when the last_value of the sequence is greater than the column maximum,"
        (let [previous-max (max-id)]
          (jdbc/execute! test-db ["DELETE FROM sequence_test WHERE id = ?" previous-max])
          (testing "reconcile-sequence-for-column! doesn't change the sequence"
            (reconcile-sequence-for-column! test-db "sequence_test" "id")
            (insert-dummy)
            (is (= (max-id)
                   (inc previous-max)))))))

    (testing "when there is no associated sequence, reconcile-sequence-for-column! throws an exception"
      (is (thrown-with-msg? Exception #"No sequence found"
                            (reconcile-sequence-for-column! test-db "authors" "name"))))))

(deftest obj->jsonb-test
  (testing "objects are converted to jsonb format successfully"
    (let [obj {:foo "bar" :baz "qux"}
          string "foobar"
          num 1234
          jsonb-string (obj->jsonb string)
          jsonb-obj (obj->jsonb obj)
          jsonb-num (obj->jsonb num)]
      (is (= org.postgresql.util.PGobject (type jsonb-string)))
      (is (= org.postgresql.util.PGobject (type jsonb-obj)))
      (is (= org.postgresql.util.PGobject (type jsonb-num)))))
  (testing "returns nil without serializing it"
    (is (nil? (obj->jsonb nil))))
  (testing "serializes nil if allow-null is specified"
    (is (= org.postgresql.util.PGobject (type (obj->jsonb nil {:allow-null true}))))))

(deftest parse-jsonb-object-test
  (testing "if the resource is not a PGobject, the original object is returned"
    (let [obj (parse-jsonb-object {:foo "bar"})
          num (parse-jsonb-object 1234)
          string (parse-jsonb-object "hello")]
      (is (map? obj))
      (is (integer? num))
      (is (string? string))))
  (testing "all PGobject values are returned, and jsonb values are returned parsed"
    (let [pgtext (doto (PGobject.)
                    (.setType "text")
                    (.setValue "hello"))
          text-result (parse-jsonb-object pgtext)
          pgjson (doto (PGobject.)
                    (.setType "json")
                    (.setValue (json/generate-string {:foo "bar"})))
          json-result (parse-jsonb-object pgjson)
          pgjsonb (doto (PGobject.)
                    (.setType "jsonb")
                    (.setValue (json/generate-string {:yee "haw"})))
          jsonb-result (parse-jsonb-object pgjsonb)]
      (is (string? text-result))
      (is (= text-result "hello"))
      (is (string? json-result))
      (is (= json-result "{\"foo\":\"bar\"}"))
      (is (map? jsonb-result))
      (is (= jsonb-result {:yee "haw"})))))

(deftest jsonb-converter-test
  (testing "the converter function only returns parsed jsonb columns from the db"
    (let [query (str "select * from jsonb_testing where text_stuff = 'text'")
          convert-all-columns {:row-fn (jsonb-converter :name :text_stuff :json_stuff :jsonb_stuff)}
          results (jdbc/query test-db [query] convert-all-columns)]
      (doseq [result results]
        (is (string? (:name result)))
        (is (string? (:text_stuff result)))
        (is (string? (:json_stuff result)))
        (is (= "{\"should\":\"return encoded\"}" (:json_stuff result)))
        (is (map? (:jsonb_stuff result)))
        (is (= {:should "return parsed"} (:jsonb_stuff result)))))))
