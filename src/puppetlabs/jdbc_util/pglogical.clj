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

;;;; Status

(def create-status-alias
  "CREATE OR REPLACE FUNCTION show_subscription_status(OUT subscription_name text, OUT status text,
     OUT provider_node text, OUT provider_dsn text, OUT slot_name text, OUT replication_sets text[],
     OUT forward_origins text[])
     RETURNS SETOF record AS $$
       SELECT * FROM pglogical.show_subscription_status();
     $$ LANGUAGE SQL
     SECURITY DEFINER;")

(defn add-status-alias
  "Adds an alias for the pglogical show_subscription_status function, allowing
  it to be called by any user, not just admin."
  [db]
  (jdbc/execute! db [create-status-alias]))

(defn consolidate-replication-status
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

(defn replica-replication-status
  "Given a DB connection for a pglogical replica node, returns
  the status of data replication. Returns :running if all subscriptions for
  that database are up and running, :disabled if any subscription has been
  disabled, :down if any connection has been severed (overrides :disabled),
  :none if pglogical replication is not configured, else :unknown. Note that
  :down may not be returned for some time or at all, depending on your
  postgresql TCP settings."
  [db]
  (if (has-pglogical-extension? db)
    (-> (jdbc/query db ["SELECT status from show_subscription_status()"] {:row-fn :status})
        (consolidate-replication-status))
    :none))

(defn consolidate-provider-status
  [statuses]
  (let [all-active? #(every? true? %)]
    (case
      (empty? statuses) :none
      (all-active? statuses) :active
      :else :inactive)))

(defn provider-replication-status
  "Given a DB connection for a pglogical provider node, query whether the
  node's subscription is currently active or not. Returns :active or
  :inactive, or :none if pglogical is not installed."
  [db]
  (if (has-pglogical-extension? db)
    (jdbc/query db ["SELECT active FROM pg_replication_slots WHERE database = ?" (:db db)])
    :none))

(defn replication-status
  "Given a DB connection for a pglogical node and the role of that node,
  :source or :replica, query replication status. Returns a map reporting the
  role and its status. Valid status values vary depending on the role; see
  other replication-status functions for more information."
  [db role]
  (let [status (if (= :source role)
                 (provider-replication status db)
                 (replica-replication-status db))]
    {:role role
     :status status}))

(defn replication-alert
  "Produces an alert that can be used if replication is down. This is only a
  function for localization reasons.

  This function takes a service name and a replication state, which can be one
  of: :disabled, :down, or :unknown. It then returns a human-readable message."
  [service-name replication-state]
  (let [construct-error (fn [message] {:severity "error"
                                       :message message})]

  (case replication-state
    :down (construct-error (tru "Database replication for {0} is currently down." service-name))
    :disabled (construct-error (tru "Database replication for {0} has been disabled." service-name))
    :unknown (construct-error (tru "Database replication for {0} is in an unknown state.")))))
