(ns puppetlabs.jdbc-util.pool
  (:require [clojure.set :as set]
            [clojure.tools.logging :as log])
  (:import [com.zaxxer.hikari HikariConfig HikariDataSource]
           java.io.Closeable
           java.sql.SQLTransientConnectionException
           javax.sql.DataSource))

(defn connection-pool
  [^HikariConfig config]
  (let [ds (HikariDataSource. config)]
    {:datasource ds}))

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
    :idle-timeout (.setIdleTimeout config value)
    :max-lifetime (.setMaxLifetime config value)
    :connection-test-query (.setConnectionTestQuery config value)
    :minimum-idle (.setMinimumIdle config value)
    :maximum-pool-size (.setMaximumPoolSize config value)
    :metric-registry (.setMetricRegistry config value)
    :health-check-registry (.setHealthCheckRegistry config value)
    :pool-name (.setPoolName config value)
    (throw (IllegalArgumentException. (format "%s is not a recognized HikariCP option" (str option))))))

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
      (dissoc :subprotocol :subname)))

(defn wrap-with-delayed-init
  "Wraps a connection pool that loops trying to get a connection, and then runs
  init-fn (with the connection as argument) before returning any connections to
  the application. Accepts a timeout in ms that's used when deferencing the
  future. The datasource should have initialization-fail-fast set before being
  created or this is pointless."
  [^DataSource datasource init-fn timeout]
  (let [pool-future
        (future
          (loop []
            (if-let [result
                     (try
                       (with-open [conn (.getConnection datasource)]
                         (init-fn conn)
                         datasource)
                       (catch SQLTransientConnectionException e
                         (log/warn e "Error while attempting to connect to database, retrying.")))]
              result
              (recur))))]
    {:datasource
     (reify
       DataSource
       (getConnection [this]
         (.getConnection (or (deref pool-future timeout nil)
                             (throw (SQLTransientConnectionException. "Timeout waiting for the database pool to become ready.")))))
       (getConnection [this username password]
         (.getConnection (or (deref pool-future timeout nil)
                             (throw (SQLTransientConnectionException. "Timeout waiting for the database pool to become ready.")))
                         username
                         password))

       Closeable
       (close [this]
         (.close datasource)))}))

(defn connection-pool-with-delayed-init
  "Create a connection pool that loops trying to get a connection, and then runs
  init-fn (with the connection as argument) before returning any connections to
  the application. Accepts a timeout in ms that's used when deferencing the
  future. This overrides the value of initialization-fail-fast and always sets
  it to false. "
  [^HikariConfig config init-fn timeout]
  (.setInitializationFailFast config false)
  (wrap-with-delayed-init (HikariDataSource. config) init-fn timeout))
