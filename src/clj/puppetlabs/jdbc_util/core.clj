(ns puppetlabs.jdbc-util.core
  (:import java.util.regex.Pattern)
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [puppetlabs.jdbc-util.cp :as cp]
            [puppetlabs.kitchensink.core :as ks]
            [schema.core :as s]))

(defn public-tables
  "Get the names of all public tables in a database"
  [db-spec]
  (let [query "SELECT table_name FROM information_schema.tables WHERE LOWER(table_schema) = 'public'"
        results (jdbc/query db-spec [query])]
    (map :table_name results)))

(defn drop-public-tables!
  "Drops all public tables in a database. Super dangerous."
  [db-spec]
  (if-let [tables (seq (public-tables db-spec))]
    (apply jdbc/db-do-commands db-spec (map #(format "DROP TABLE %s CASCADE" %) (seq tables)))))

(defn convert-result-arrays
  "Converts Java and JDBC arrays in a result set using the provided
  function (eg. vec, set). Values which aren't arrays are unchanged."
  ([result-set]
   (convert-result-arrays vec result-set))
  ([f result-set]
   (let [convert #(cond
                    (ks/array? %) (f %)
                    (isa? (class %) java.sql.Array) (f (.getArray %))
                    :else %)]
     (map #(ks/mapvals convert %) result-set))))

(defn query
  "An implementation of query that returns a fully evaluated result (no
  JDBCArray objects, etc)"
  [db sql-and-params]
  (let [convert (fn [rs]
                  (doall
                    (convert-result-arrays vec
                                           (jdbc/result-set-seq rs))))]
    (jdbc/db-query-with-resultset db sql-and-params convert)))

(defn ordered-group-by
  [f coll]
  (let [grouped-w-index (loop [i 0, groups (transient {}), coll (seq coll)]
                          (if-let [x (first coll)]
                            (let [k (f x)
                                  group (get groups k [i])
                                  groups' (assoc! groups k (conj group x))]
                              (recur (inc i) groups' (next coll)))
                            ;; else (nothing left in coll)
                            (persistent! groups)))]
    (->> (seq grouped-w-index)
      ; sort the groups by the index where the first member appeared
      (sort-by #(get-in % [1 0])))))

(defn aggregate-submap-by
  "Given a sequence of maps in results where each map contains agg-key
  and agg-val as keys, groups the maps that are identical except for the
  values in agg-key or agg-val. The values of agg-key and agg-val are
  turned into a map and stored in the resulting map under under-key."
  [agg-key agg-val under-key results]
  (for [[combined [_ & all]] (ordered-group-by #(dissoc % agg-key agg-val) results)]
    (assoc combined under-key (->> all
                                (map (juxt agg-key agg-val))
                                (remove (comp nil? first))
                                (into {})))))

(defn aggregate-column
  "Given a sequence of rows as maps, aggregate the values of `column`
  into a sequence under `under`, combining rows that are equal except
  for the value of `column`. Useful for consolidating the results of an
  outer join."
  [column under results]
  (for [[combined [_ & all]] (ordered-group-by #(dissoc % column) results)]
    (assoc combined under (map #(get % column) all))))

(defn- sequence-placeholder
  [xs]
  (str "("
       (->> (repeat (count xs) "?")
         (str/join ","))
       ")"))

(defn- replace-nth-?
  [^String s n replacement]
  (let [through-?-pattern (Pattern/compile (format "([^?]+?\\?){%d}" (inc n)))]
    (if-let [[match] (re-find through-?-pattern s)]
      (let [tail (.substring s (.length ^String match) (.length s))
            replaced (str/replace match #"(.*)\?$" (str "$1" replacement))]
        (str replaced tail))
      (throw (IllegalArgumentException. (str "there are not " n " '?'s in the given string"))))))

(defn expand-seq-params
  "A helper for prepared SQL statements with sequential parameters.
  Returns a new prepared statement with every `?` that corresponded to a
  sequential parameter expanded to a tuple literal of the appropriate
  length and flattened parameters."
  [[sql & parameters]]
  (let [seq-params-w-indices (->> (map vector parameters (range))
                               (filter (comp sequential? first)))
        [sql'] (reduce (fn [[sql shift] [param i]]
                         (let [shift' (+ shift (dec (count param)))
                               expansion (sequence-placeholder param)]
                           [(replace-nth-? sql (+ i shift) expansion) shift']))
                       [sql 0]
                       seq-params-w-indices)]
    (vec (conj (flatten parameters) sql'))))

;; Connection pooling

(s/defn ^:always-validate pooled-db-spec :- cp/pooled-db-spec-schema
  "Given a database connection attribute map, return a JDBC datasource
  compatible with clojure.java.jdbc that is backed by a connection
  pool."
  [pool-name options]
  (cp/pooled-db-spec pool-name options))

(s/defn ^:always-validate close-pooled-db-spec
  "Close an open DataSource supplied as a JDBC compatible db-spec."
  [db-spec :- cp/pooled-db-spec-schema]
  (cp/close-pooled-db-spec db-spec))
