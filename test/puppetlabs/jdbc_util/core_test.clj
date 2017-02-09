(ns puppetlabs.jdbc-util.core-test
  (:import [java.util UUID]
           [org.postgresql.util PSQLException PSQLState])
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

(deftest pg-sql-escape-test
  (testing "pg-sql-escape"
    (testing "allows \"Robert'); DROP TABLE students;--\" to be safely inserted"
      (let [bobby-tables "Robert'); DROP TABLE students;--"
            color (ks/rand-str :alpha-lower 2)]
        (jdbc/execute! test-db [(format "INSERT INTO authors (name, favorite_color) VALUES (%s, '%s')"
                                        (pg-sql-escape bobby-tables) color)])
        (is (= {:name bobby-tables}
               (first (jdbc/query test-db ["SELECT name FROM authors WHERE favorite_color = ?"
                                           color]))))
        (jdbc/execute! test-db ["DELETE FROM authors WHERE favorite_color = ?" color])))

    (testing "handles strings that try to close the dollar quoting themselves"
      (dotimes [i 100]
        (let [rand-tag (if (zero? i)
                         ""
                         (ks/rand-str :alpha 5))
              malicious-string (str "$" rand-tag "$); DROP TABLE jdbc_util_test;--")
              escaped (pg-sql-escape malicious-string)]
          (is (= malicious-string
                 (-> (jdbc/query test-db [(format "SELECT %s AS string" escaped)])
                   first
                   :string))))))

    (testing "produces escaped strings that satisfy the expected properties"
      (tc/quick-check
        10000 ; 10k
        (tc-prop/for-all [s tc-gen/string]
          (let [escaped (pg-sql-escape s)
                [delim0 tag0] (re-find #"^\$([^$]+)\$" escaped)
                [delim1 tag1] (re-find #"\$([^$]+)\$$" escaped)
                delim-occurrences (re-seq (re-pattern (str "\\$" tag0 "\\$")) escaped)]
            (and delim0 tag0 delim1 tag1 ; escaped string has delimiters at beginning & end
                 (= delim0 delim1) ; delimiters are the same at beginning & end
                 (= 2 (count delim-occurrences)) ; delimiters only appear at beginning & end
                 (.contains escaped s)))))))) ; escaped string contains original string

(defn- randomly-insert-from
  [character-set]
  (fn rand-insert
    ([s] (rand-insert s (inc (rand-int 5))))
    ([s insertions]
     (let [s-length (.length s)
           i (rand-int (inc s-length))
           s' (str (subs s 0 i)
                   (rand-nth character-set)
                   (subs s i s-length))]
       (if (<= insertions 1)
         s'
         (recur s' (dec insertions)))))))

(deftest safe-pg-identifier?-test
  (testing "safe-pg-identifier?"
    (testing "rejects names with symbols besides underscores and hyphens"
      (let [bad-symbols (-> (:symbols ks/ascii-character-sets)
                          set
                          (disj \- \_)
                          vec)
            insert-bad-symbols (randomly-insert-from bad-symbols)]
        (tc/quick-check
          10000
          ;; shrinking doesn't work because of this here fmap of a random fn
          (tc-prop/for-all [s (tc-gen/fmap insert-bad-symbols tc-gen/string-alphanumeric)]
            (false? (safe-pg-identifier? s))))))

    (testing "accepts names with digits, underscores, hyphens, and alphabetical characters"
      (are [nombre] (true? (safe-pg-identifier? nombre))
        "zaphod42"
        "righteous-lisp-style"
        "profane_c_convention"
        "αβγ"
        "Přílišžluťoučkýkůňúpělďábelskéódy"
        "YxskaftbudgevårWC-zonmöIQ-hjälp"
        "Эхчужакобщийсъёмценшляпюфтьвдрызг"
        "หัดอภัยเหมือนกีฬาอัชฌาสัย"
        "लाल"
        "石室诗士施氏嗜狮誓食十狮"))))

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

(defn- rand-db-name [] (str "jdbc-util-test-db-" (ks/rand-str :alpha-lower 12)))

(deftest create-db!-test
  (testing "create-db!"
    (let [rand-db (rand-db-name)]
      (is (false? (db-exists? test-db rand-db)))
      (create-db! test-db rand-db (:user test-db))
      (is (true? (db-exists? test-db rand-db)))
      (jdbc/execute! test-db [(format "DROP DATABASE \"%s\"" rand-db)]
                     {:transaction? false}))

    (testing "appears to use `safe-pg-identifier?` to screen"
      (testing "DB names"
        (is (thrown? AssertionError (create-db! test-db "what\"quote" "guccifer"))))
      (testing "user names"
        (is (thrown? AssertionError (create-db! test-db "school" "Robert'); DROP TABLE students;")))))

    (testing "throws an error when given a db that already exists"
      (let [rand-db (rand-db-name)]
        (create-db! test-db rand-db (:user test-db))
        (is (thrown-with-msg? PSQLException #"already exists"
                              (create-db! test-db rand-db (:user test-db))))
        (jdbc/execute! test-db [(format "DROP DATABASE \"%s\"" rand-db)]
                       {:transaction? false})))))

(deftest drop-db!-test
  (testing "drop-db!"
    (testing "works"
      (let [rand-db (rand-db-name)]
        (jdbc/execute! test-db [(format "CREATE DATABASE \"%s\"" rand-db)]
                       {:transaction? false})
        (is (true? (db-exists? test-db rand-db)))
        (is (nil? (drop-db! test-db rand-db)))
        (is (false? (db-exists? test-db rand-db)))))

    (testing "appears to use `safe-pg-identifier?` to screen DB names"
      (is (thrown? AssertionError (drop-db! test-db "sad-times;_;"))))

    (testing "doesn't throw when given a database that doesn't exist"
      (is (nil? (drop-db! test-db "no-such-database"))))))

(deftest user-exists?-test
  (is (true? (user-exists? test-db (:user test-db))))
  (is (false? (user-exists? test-db "Waldo"))))

(defn- rand-username [] (str "jdbc-util-test-user-" (ks/rand-str :alpha-lower 12)))

(deftest create-user!-test
  (testing "create-user!"
    (testing "works"
      (let [rand-user (rand-username)]
        (is (false? (user-exists? test-db rand-user)))
        (create-user! test-db rand-user "badpassword")
        (is (true? (user-exists? test-db rand-user)))
        (jdbc/execute! test-db [(format "DROP USER \"%s\"" rand-user)])))

    (testing "appears to use `safe-pg-identifier?` to screen user names"
      (is (thrown? AssertionError
                   (create-user! test-db "eve;PATH=/usr/bin/compromised:$PATH" "123"))))

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
        (is (nil? (create-user! test-db (rand-username) "';$$; DROP TABLE users")))))))

(deftest drop-user!-test
  (testing "drop-user!"
    (testing "works"
      (let [rand-user (rand-username)]
        (jdbc/execute! test-db [(format "CREATE USER \"%s\"" rand-user)]
                       {:transaction? false})
        (is (true? (user-exists? test-db rand-user)))
        (drop-user! test-db rand-user)
        (is (false? (user-exists? test-db rand-user)))))

    (testing "appears to use `safe-pg-identifier?` to screen user names"
      (is (thrown? AssertionError (drop-user! test-db "mallory$(rm -rf /)"))))

    (testing "doesn't throw when given a user that doesn't exist"
      (is (nil? (drop-user! test-db (rand-username)))))))

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
