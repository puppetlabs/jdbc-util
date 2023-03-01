(ns puppetlabs.jdbc-util.pool
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log]
            [puppetlabs.i18n.core :refer [tru trs trun]]
            [puppetlabs.jdbc-util.migration :as migration])
  (:import com.codahale.metrics.health.HealthCheckRegistry
           [com.zaxxer.hikari HikariConfig HikariDataSource]
           java.io.Closeable
           [java.sql SQLTransientConnectionException SQLTransientException SQLException]
           javax.sql.DataSource
           (java.util.concurrent ExecutionException)))

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

(defprotocol PoolLifetime
  (block-until-ready [this] [this timeout-ms]
    "Block execution until initialization is done, or the timeout expires (if specified).
    If the timeout is specified, returns true if the execution completed within the timeperiod,
    false if it didn't.")
  (cancel-init [this] "Attempt to cancel the async initialization.  Returns true if it was cancelled, false otherwise")
  (init-complete? [this] "Returns true if the init is complete, false otherwise")
  (close-after-ready [this] [this timeout-ms]
    "Wait for the init routine to complete and then close the datasource. If the timeout is specified and expires
    before the init is complete, cancel the init and close the datasource."))

(def replica-migrations-polling-interval-ms 1500)

(defn wait-for-migrations
  "Loops until there are no uncompleted migrations from the migration-dir in the
  db. We use this on pglogical replicas which will have their database
  migrations replicated."
  [db migration-dir]
  (loop []
    (when (not-empty (migration/uncompleted-migrations db migration-dir))
      (Thread/sleep replica-migrations-polling-interval-ms)
      (recur))))

(defn wrap-with-delayed-init
  "Wraps a connection pool that loops trying to get a connection, and then runs
  database migrations, then calls init-fn (with the connection as argument)
  before returning any connections to the application. Accepts a timeout in ms
  that's used when dereferencing the future and by the status check. The
  datasource should have initialization-fail-fast set before being created or
  this is pointless.

  migration-opts is a map of:
    :migration-dir, the path to the migratus migration directory on the classpath
    :migration-dirs, the optional list of paths to any migratus migration directories
    :migration-db, the connection map for the db used for migrations
    :replication-mode, one of :source, :replica, or :none (the default)

  If migration-opts is nil or not passed, the migration step is skipped."
  ([^HikariDataSource datasource init-fn timeout]
   (wrap-with-delayed-init datasource nil init-fn timeout))
  ([^HikariDataSource datasource migration-opts init-fn timeout]
   (let [init-error (atom nil)
         init-exited-safely (promise)
         pool-future
         (future
          (log/debug (trs "{0} - Starting database initialization" (.getPoolName datasource)))
          (loop []
             (if-let [result
                      (try
                        ;; Try to get a connection to make sure the db is ready
                        (.close (.getConnection datasource))
                        (let [{:keys [migration-db migration-dir migration-dirs]} migration-opts]
                          ;; ensure both the db and a migration directory is specified
                          (if (and migration-db (or migration-dir migration-dirs))
                            (do
                              (log/debug (trs "{0} - Starting database migration" (.getPoolName datasource)))
                              ;; If we're a replica then pglogical will be
                              ;; replicating our migrations for us, so we poll until
                              ;; the migrations have been replicated
                              (let [migration-dirs (if migration-dirs migration-dirs [migration-dir])]
                                (if (= (:replication-mode migration-opts) :replica)
                                  (doseq [single-dir migration-dirs]
                                    (wait-for-migrations migration-db single-dir))
                                  (doseq [single-dir migration-dirs]
                                    (log/info (trs "migrating from {0}" single-dir))
                                    (migration/migrate migration-db single-dir)))))
                            (log/info (trs "{0} No migration path specified, skipping database migration." (.getPoolName datasource)))))

                        (log/debug (trs "{0} - Starting post-migration init-fn" (.getPoolName datasource)))
                        (init-fn {:datasource datasource})
                        (log/debug (trs "{0} - Finished database migration" (.getPoolName datasource)))
                        datasource
                        (catch SQLTransientException e
                               (log/warnf e (trs "{0} - Error while attempting to connect to database, retrying.")
                                   (.getPoolName datasource))
                               nil)
                        (catch Exception e
                          (reset! init-error e)
                          (log/errorf e (trs "{0} - An error was encountered during database migration."
                                             (.getPoolName datasource)))
                          ;; return the datasource so we know we are done.
                          datasource))]
               (do
                 (deliver init-exited-safely true)
                 result)

               (recur))))]
     (reify
       DataSource
       (getConnection [this]
         (if (deref init-error)
           (throw (RuntimeException. (tru "Unrecoverable error occurred during database initialization.")))
           (.getConnection (or (deref pool-future timeout nil)
                               (throw (SQLTransientConnectionException. (tru "Timeout waiting for the database pool to become ready.")))))))
       (getConnection [this username password]
         (if (deref init-error)
           (throw (RuntimeException. (tru "Unrecoverable error occurred during database initialization.")))
           (.getConnection (or (deref pool-future timeout nil)
                               (throw (SQLTransientConnectionException. (tru "Timeout waiting for the database pool to become ready."))))
                           username
                           password)))

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
         @init-error)

       PoolLifetime
       (block-until-ready [this]
         (log/info (trs "{0} - Blocking execution until db init has finished" (.getPoolName datasource)))
         (try
           (deref pool-future)
           (catch ExecutionException e
             (log/warn e (trs "{0} - Exception generated during init" (.getPoolName datasource))))))
       (block-until-ready [this timeout-ms]
         (log/info (trs "{0} - Blocking execution until db init has finished with {1} millisecond timeout "
                        (.getPoolName datasource) timeout-ms))
         (try
          (not (nil? (deref pool-future timeout-ms nil)))
          (catch ExecutionException e
            (log/warn e (trs "{0} - Exception generated during init" (.getPoolName datasource)))
            true)))
       (cancel-init [this]
         (future-cancel pool-future))
       (init-complete? [this]
         (future-done? pool-future))
       (close-after-ready [this]
         (block-until-ready this)
         (.close datasource))
       (close-after-ready [this timeout-ms]
         (when-not (block-until-ready this timeout-ms)
           (log/warn (trs "{0} - Cancelling db-init due to timeout" (.getPoolName datasource)))
           (cancel-init this)
           ; since we have cancelled the init, we need to specifically wait until the migrations have exited
           ; safely before closing the connection
           (deref init-exited-safely timeout-ms :timeout)
           (log/info (trs "{0} - Done waiting for init safe exit" (.getPoolName datasource))))
         (.close datasource))))))

(defn connection-pool-with-delayed-init
  "Create a connection pool that loops trying to get a connection, and then runs
  init-fn (with the connection as argument) before returning any connections to
  the application. Accepts a timeout in ms that's used when deferencing the
  future. This overrides the value of initialization-fail-timeout to never timeout. "
  ([^HikariConfig config init-fn timeout]
   (connection-pool-with-delayed-init config nil init-fn timeout))
  ([^HikariConfig config migration-options init-fn timeout]
   (.setInitializationFailTimeout config -1)
   (when-not (.getHealthCheckRegistry config)
     (.setHealthCheckRegistry config (HealthCheckRegistry.)))
   (wrap-with-delayed-init (HikariDataSource. config) migration-options init-fn timeout)))
