(ns puppetlabs.jdbc-util.pool
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [puppetlabs.i18n.core :refer [tru trs trun]]
            [puppetlabs.jdbc-util.migration :as migration])
  (:import com.codahale.metrics.health.HealthCheckRegistry
           [com.zaxxer.hikari HikariConfig HikariDataSource]
           java.io.Closeable
           [java.sql SQLTransientConnectionException SQLTransientException]
           javax.sql.DataSource))

(defn add-connectivity-check-timeout-ms
  [^HikariConfig config timeout]
  (.addHealthCheckProperty config "connectivityCheckTimeoutMs" (str timeout))
  config)

(defn- set-option
  [^HikariConfig config option value]
  (case option
    :username (.setUsername config value)
    :password (.setPassword config value)
    :data-source-class-name (.setDataSourceClassName config value)
    :jdbc-url (.setJdbcUrl config value)
    :driver-class-name (.setDriverClassName config value)
    :auto-commit (.setAutoCommit config value)
    :connection-timeout (.setConnectionTimeout config value)
    :connection-check-timeout (add-connectivity-check-timeout-ms config value)
    :idle-timeout (.setIdleTimeout config value)
    :max-lifetime (.setMaxLifetime config value)
    :connection-test-query (.setConnectionTestQuery config value)
    :minimum-idle (.setMinimumIdle config value)
    :maximum-pool-size (.setMaximumPoolSize config value)
    :metric-registry (.setMetricRegistry config value)
    :health-check-registry (.setHealthCheckRegistry config value)
    :pool-name (.setPoolName config value)
    (throw (IllegalArgumentException. (tru "{0} is not a supported HikariCP option" (str option))))))

(defn options->hikari-config
  [options]
  (let [config (HikariConfig.)]
    (doseq [[option value] options]
      (set-option config option value))
    (.validate config)
    config))

(defn spec->hikari-options
  [db-spec]
  (-> db-spec
      (set/rename-keys {:user :username
                        :classname :driver-class-name})
      (assoc :jdbc-url (str "jdbc:"
                            (:subprotocol db-spec) ":"
                            (:subname db-spec)))
      (dissoc :subprotocol :subname
              :migration-password :migration-user)))

(defn select-user-configurable-hikari-options
  "Given a map, return the subset of entries in the map whose keys are hikari
  options that we want our users to be able to configure. This is intended to
  allow users to set these fields in the database configuration section of a
  service's TrapperKeeper config."
  [m]
  (select-keys [:connection-timeout
                :connection-check-timeout
                :idle-timeout
                :max-lifetime
                :minimum-idle
                :maximum-pool-size]
               m))

(defprotocol PoolStatus
  (status [this] "Get a map representing the status of a connection pool.")
  (init-error [this] "Return any exception raised by the init function (nil if none)."))

(defn wrap-with-delayed-init
  "Wraps a connection pool that loops trying to get a connection, and then runs
  init-fn (with the connection as argument) before returning any connections to
  the application. Accepts a timeout in ms that's used when dereferencing the
  future and by the status check. The datasource should have
  initialization-fail-fast set before being created or this is pointless.

  migration-opts is a map of:
    :migration-dir, the path to the migratus migration directory on the classpath
    :migration-db, the connection map for the db used for migrations

  If migration-opts is nil or not passed, the migration step is skipped."
  ([^HikariDataSource datasource init-fn timeout]
   (wrap-with-delayed-init datasource nil init-fn timeout))
  ([^HikariDataSource datasource migration-opts init-fn timeout]
   (when-not (.getHealthCheckRegistry datasource)
     (.setHealthCheckRegistry datasource (HealthCheckRegistry.)))
   (let [init-error (atom nil)
         pool-future
         (future
           (loop []
             (if-let [result
                      (try
                        ;; Try to get a connection to make sure the db is ready
                        (.close (.getConnection datasource))
                        (when-let [{:keys [migration-db migration-dir]} migration-opts]
                          (try
                            (migration/migrate migration-db migration-dir)
                            (catch Exception e
                              (reset! init-error e)
                              (log/errorf e (trs "{0} - An error was encountered during database migration."
                                                 (:subname migration-db "unknown"))))))
                        (try
                          (init-fn {:datasource datasource})
                          (catch Exception e
                            (reset! init-error e)
                            (log/errorf e (trs "{0} - An error was encountered during initialization."
                                               (.getPoolName datasource)))))
                        datasource
                        (catch SQLTransientException e
                          (log/warnf e (trs "{0} - Error while attempting to connect to database, retrying."
                                            (.getPoolName datasource)))
                          nil))]
               result
               (recur))))]
     (reify
       DataSource
       (getConnection [this]
         (.getConnection (or (deref pool-future timeout nil)
                             (throw (SQLTransientConnectionException. (tru "Timeout waiting for the database pool to become ready."))))))
       (getConnection [this username password]
         (.getConnection (or (deref pool-future timeout nil)
                             (throw (SQLTransientConnectionException. (tru "Timeout waiting for the database pool to become ready."))))
                         username
                         password))

       Closeable
       (close [this]
         (.close datasource))

       PoolStatus
       (status [this]
         (if (realized? pool-future)
           (let [connectivity-check (str (.getPoolName datasource)
                                         ".pool.ConnectivityCheck")
                 health-result (.runHealthCheck
                                (.getHealthCheckRegistry datasource)
                                connectivity-check)
                 healthy? (and (.isHealthy health-result)
                               (nil? @init-error))
                 messages (remove nil? [(some->> @init-error
                                                 (.getMessage)
                                                 (tru "Initialization resulted in an error: {0}"))
                                        (.getMessage health-result)])]
             (cond-> {:state (if healthy?
                               :ready
                               :error)}
               (not healthy?) (merge {:messages messages})))
           {:state :starting}))

       (init-error [this]
         @init-error)))))

(defn connection-pool-with-delayed-init
  "Create a connection pool that loops trying to get a connection, and then runs
  init-fn (with the connection as argument) before returning any connections to
  the application. Accepts a timeout in ms that's used when deferencing the
  future. This overrides the value of initialization-fail-fast and always sets
  it to false. "
  ([^HikariConfig config init-fn timeout]
   (connection-pool-with-delayed-init config nil init-fn timeout))
  ([^HikariConfig config migration-options init-fn timeout]
   (let [datasource (HikariDataSource. config)]
     (.setInitializationFailFast config false)
     (wrap-with-delayed-init datasource migration-options init-fn timeout))))
