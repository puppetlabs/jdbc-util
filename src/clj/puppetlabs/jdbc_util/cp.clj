(ns puppetlabs.jdbc-util.cp
  "Connection pooling specific internals."
  (:import com.codahale.metrics.MetricRegistry
           com.zaxxer.hikari.HikariConfig
           com.zaxxer.hikari.HikariDataSource
           org.jdbcdslog.ConnectionPoolDataSourceProxy)
  (:require [metrics.core :as metrics]
            [schema.core :as s]))

;; Defaults

(def default-datasource-options
  {;; Most clojure.java.jdbc usage means we don't want this on by default.
   :auto-commit              false
   ;; This means that by default, we retry until connection-timeout when
   ;; initializing the pool.
   :initialization-fail-fast false
   ;; We presume always enabling JMX is a good idea
   :register-mbeans          true
   ;; Without explicit transaction isolation and a test query, we are unable
   ;; to change the transaction isolation level on a per query basis since
   ;; Connection.isAlive() tests aren't transactionally isolated.
   :connection-test-query    "/* hikaricp test query */ select 1;"
   :isolate-internal-queries true
   ;; Statistics default settings
   :stats                    true
   :metrics-registry         metrics/default-registry})

;; Schema

(def pooled-db-spec-schema
  "Schema representing a clojure.java.jdbc compatible pooled db-spec."
  {:datasource javax.sql.DataSource})

(defn- gte-0?
  "Returns true if num is greater than or equal 0, else false"
  [x]
  (>= x 0))

(defn- gte-1?
  "Returns true if num is greater than or equal 1, else false"
  [x]
  (>= x 1))

(defn- gte-1000?
  "Returns true if num is greater than or equal 1000, else false"
  [x]
  (>= x 1000))

(def ^{:private true} IntGte0
  (s/both s/Int (s/pred gte-0? 'gte-0?)))

(def ^{:private true} IntGte1
  (s/both s/Int (s/pred gte-1? 'gte-1?)))

(def ^{:private true} IntGte1000
  (s/both s/Int (s/pred gte-1000? 'gte-1000?)))

(def ConfigurationOptions
  {:auto-commit                         s/Bool
   (s/optional-key :classname)          s/Str
   (s/optional-key :connection-timeout) IntGte1000
   (s/optional-key :idle-timeout)       IntGte0
   :initialization-fail-fast            s/Bool
   (s/optional-key :max-lifetime)       IntGte0
   (s/optional-key :maximum-pool-size)  IntGte1
   :metrics-registry                    MetricRegistry
   (s/optional-key :minimum-idle)       IntGte0
   (s/optional-key :password)           s/Str
   :pool-name                           s/Str
   (s/optional-key :read-only)          (s/maybe s/Bool)
   :register-mbeans                     s/Bool
   :stats                               s/Bool
   :subname                             s/Str
   :subprotocol                         s/Str
   (s/optional-key :validation-timeout) IntGte1000
   (s/optional-key :user)               s/Str
   :connection-test-query               s/Str
   :isolate-internal-queries            s/Bool})

;; Functions

(s/defn ^:always-validate add-datasource-property
  "Add a custom datasource property to the underlying database driver."
  [config :- HikariConfig
   property :- s/Str
   value :- s/Str]
  (.addDataSourceProperty config property value))

(s/defn ^:always-validate add-datasource-properties
  "Like add-datasource-property, but allows you to pass a map of properties and
   values."
  [config :- HikariConfig
   properties :- {s/Str s/Str}]
  (doseq [[k v] properties]
    (add-datasource-property config k v)))

(defn validate-options
  "Manage defaulting and validation of options."
  [options]
  (s/validate ConfigurationOptions (merge default-datasource-options options)))

(s/defn ^:always-validate datasource-config :- HikariConfig
  "Produce a valid HikariConfig object from provided configuration options."
  [datasource-options]
  (let [config (HikariConfig.)
        options (validate-options datasource-options)
        {:keys [adapter
                auto-commit
                connection-test-query
                connection-timeout
                idle-timeout
                initialization-fail-fast
                internal-test-query
                isolate-internal-queries
                max-lifetime
                maximum-pool-size
                metrics-registry
                minimum-idle
                password
                pool-name
                read-only
                register-mbeans
                stats
                subname
                subprotocol
                user
                validation-timeout]} options]

    ;; Set pool-specific properties
    (doto config
      (.setAutoCommit auto-commit)
      (.setInitializationFailFast initialization-fail-fast)
      (.setRegisterMbeans register-mbeans)
      (.setJdbcUrl (str "jdbc:" subprotocol ":" subname))
      (.setIsolateInternalQueries isolate-internal-queries)
      (.setConnectionTestQuery connection-test-query)
      (.setPoolName pool-name))

    ;; Set optional properties
    (when stats (.setMetricRegistry config metrics-registry))
    (when read-only (.setReadOnly config read-only))
    (when idle-timeout (.setIdleTimeout config idle-timeout))
    (when max-lifetime (.setMaxLifetime config max-lifetime))
    (when minimum-idle (.setMinimumIdle config minimum-idle))
    (when maximum-pool-size (.setMaximumPoolSize config maximum-pool-size))
    (when validation-timeout (.setValidationTimeout config validation-timeout))
    (when connection-timeout (.setConnectionTimeout config connection-timeout))
    (when user (.setUsername config user))
    (when password (.setPassword config password))

    ;; Set driver specific properties
    (case subprotocol
      "postgresql" (add-datasource-properties config {"ApplicationName" pool-name
                                                      "tcpKeepAlive" "true"
                                                      "logUnclosedConnections" "true"})
      "default")

    config))

(defn pooled-db-spec
  "Create a pooled clojure.java.jdbc compatible db-spec."
  [pool-name options]
  (let [options (assoc options :pool-name pool-name)
        config (datasource-config options)
        orig-ds (HikariDataSource. config)
        jdbcdslog (ConnectionPoolDataSourceProxy.)
        _ (doto jdbcdslog
            (.setTargetDSDirect orig-ds))]
    {:datasource jdbcdslog}))

(s/defn ^:always-validate close-pooled-db-spec
  "Close an open DataSource supplied as a clojure.java.jdbc compatible db-spec."
  [db-spec :- pooled-db-spec-schema]
  (let [ds (:datasource db-spec)
        ;; Retrieve target DS, if we are using the connection proxy
        ds (if (instance? org.jdbcdslog.ConnectionPoolDataSourceProxy ds)
             (.getTargetDSDirect ds)
             ds)]
    (.close ds)))
