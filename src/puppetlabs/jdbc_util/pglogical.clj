(ns puppetlabs.jdbc-util.pglogical
  (:import [org.postgresql.util PSQLException PSQLState])
  (:require [clojure.java.jdbc :as jdbc]
            [puppetlabs.i18n.core :refer [tru]]
            [puppetlabs.jdbc-util.core :refer [has-extension?]]))

(defn has-pglogical-extension? [db]
  (has-extension? db "pglogical"))

(defn- unsafe-escape-sql-quotes
  "Escape the given string so it can be \"safely\" passed as a string parameter
  to an sql query."
  [s]
  (clojure.string/replace s "'" "''"))

(defn wrap-ddl-for-pglogical
  "Wrap the given sql (presumably DDL) in a call to
  pglogical.replicate_ddl_command, escaping quotes and wrapping the statement so
  it won't return anything."
  [sql schema]
  (str "do 'begin perform "
       (unsafe-escape-sql-quotes
        (str "pglogical.replicate_ddl_command('"
             "set local search_path to " schema "; "
             (unsafe-escape-sql-quotes sql)
             "');"))
       " end;';"))

(defn update-pglogical-replication-set
  "Tries to update the default pglogical replication set to replicate all tables
  in the given schema. If the db user don't have rights to update pglogical,
  catches the exception and returns false. Other exceptions are passed through.
  Returns true if the replication set was successfully updated."
  [db schema]
  (try
    (jdbc/query db
                (str "select pglogical.replication_set_add_all_tables("
                     "'default',"
                     "'{\"" schema "\"}',"
                     "true"
                     ");"))
    true
    (catch PSQLException e
      (if (= "42501" (.getSQLState e))
        false
        (throw e)))))


;;; Status internals

(def ^:private create-status-alias-sql
  "CREATE OR REPLACE FUNCTION
     show_subscription_status(
       OUT subscription_name text,
       OUT status text,
       OUT provider_node text,
       OUT provider_dsn text,
       OUT slot_name text,
       OUT replication_sets text[],
       OUT forward_origins text[])
     RETURNS SETOF record AS $$
       SELECT * FROM pglogical.show_subscription_status();
     $$ LANGUAGE SQL
     SECURITY DEFINER;")

(defn consolidate-replica-status
  "Given a list of states of pglogical subscriptions, returns
  the status of data replication. Returns :running if all subscriptions for
  that database are up and running, :disabled if any subscription has been
  turned off, :down if any connection has been severed (overrides :disabled),
  :none if pglogical replication is not configured, or :unknown. Note that
  :down may not be returned for some time or at all, depending on your
  postgresql TCP settings."
  [statuses]
  (let [all-good? #(every? (partial = "replicating") %)
        disabled? #(some (partial = "disabled") %)
        down? #(some (partial = "down") %)]
    (cond
      (empty? statuses) :none
      (all-good? statuses) :running
      (down? statuses) :down
      (disabled? statuses) :disabled
      :else :unknown)))

(defn- replica-replication-status
  "Given a DB connection for a pglogical replica node, returns
  the status of data replication. Returns :running if all subscriptions for
  that database are up and running, :disabled if any subscription has been
  disabled, :down if any connection has been severed (overrides :disabled),
  :none if pglogical replication is not configured, else :unknown. Note that
  :down may not be returned for some time or at all, depending on your
  postgresql TCP settings."
  [db]
  (if (has-pglogical-extension? db)
    (->> (jdbc/query db ["SELECT status from show_subscription_status()"] {:row-fn :status})
         consolidate-replica-status)
    :none))

(defn- consolidate-provider-status
  "Given a seq of boolean replication slot statuses (true == active, false ==
  inactive), return the overall provder status; one of :active, :inactive,
  or :none."
  [statuses]
  (let [all-active? #(every? true? %)]
    (case
        (empty? statuses) :none
        (all-active? statuses) :active
        :else :inactive)))

(defn- provider-replication-status
  "Given a DB connection for a pglogical provider node, query whether the
  node's subscription is currently active or not. Returns :active or
  :inactive, or :none if pglogical is not installed."
  [db]
  (if (has-pglogical-extension? db)
    (->> (jdbc/query db ["SELECT active FROM pg_replication_slots WHERE database = current_database()"] {:row-fn :active})
         consolidate-provider-status)
    :none))

(defn- replication-status
  "Given a DB connection for a pglogical node and the replication mode of :none,
  :source or :replica, query replication status. Returns a status keyword,
  or :none if replication-mode is :none. Valid status values vary depending on
  thew role; see other replication-status functions for more information."
  [db replication-mode]
  (case replication-mode
    :source (provider-replication-status db)
    :replica (replica-replication-status db)
    :none :none
    nil :none))

(defn- format-replication-alert
  "Produce an alert that can be used if replication is down.

  This function takes a service name and a replication status, which can be any
  value returned by one of the replication-status functions above. It then
  returns an alert map as required by tk-status, including a localized
  human-readable message.

  Returns nil if there's nothing wrong."
  [service-name replication-status]
  (when-let [status-message (case replication-status
                              :down (tru "Database replication for {0} is currently down." service-name)
                              :disabled (tru "Database replication for {0} has been disabled." service-name)
                              :unknown (tru "Database replication for {0} is in an unknown state." service-name)
                              :inactive (tru "Database replication for {0} is inactive" service-name)
                              nil)]
    {:severity :error
     :message status-message}))


;;; Status public API

(defn add-status-alias
  "Create a function which wraps the pglogical show_subscription_status
  function, allowing it to be called by any user, not just admin. This function
  is used by pglogical replication status, and is required for it to function."
  [db schema]
  (jdbc/execute! db [(if (has-pglogical-extension? db)
                       (wrap-ddl-for-pglogical create-status-alias-sql schema)
                       create-status-alias-sql)]))

(defn combined-replication-status
  "Return a map with of pglogical status information suitable for inclusion in a
  tk-status response. It looks like this:

  {:alerts [{:severity :error, :message \"human readable message\"]
   :structured-status {:mode (:source, :replica, or :none)
                       :status (:running, :down, :disabled, :unknown, :inactive, or :none)}

  The seq at the alerts key may be nil if there's nothing wrong. It should be
  combined with any application-specific alerts and returned in the :alerts
  section of the tk-status response.

  The value at the :structured-status key should be included in the tk-status
  response at the path [:status :replication]."
  [db replication-mode service-name]
  (let [status (replication-status db replication-mode)]
    {:alerts (some-> (format-replication-alert service-name status)
                     vector)
     :structured-status {:mode replication-mode
                         :status status}}))
