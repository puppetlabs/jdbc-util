(ns puppetlabs.jdbc-util.migration
  (:import java.sql.BatchUpdateException)
  (:require [clojure.tools.logging :as log]
            [migratus.core :as migratus]
            [migratus.protocols :as mproto]
            [puppetlabs.i18n.core :as i18n]
            [puppetlabs.jdbc-util.pglogical :as pglogical]))


(defn spec->migration-db-spec
  "Given a user defined database config, transform the config into a db-spec
  appropriate for passing to migratus's migrate function."
  [db-config]
  (let [?user (or (:migration-user db-config)
                  (:user db-config))
        ?password (if (:migration-user db-config)
                    (:migration-password db-config)
                    (:password db-config))]
    (cond-> db-config
      :always   (dissoc :password, :migration-user, :migration-password)
      ?user     (assoc :user ?user)
      ?password (assoc :password ?password))))

(defn migrate
  "Migrate 'db' using migratus with a given 'migration-dir'."
  [db migration-dir]
  (let [have-pglogical (pglogical/has-pglogical-extension? db)
        pg-schema "public"
        config {:store :database
                :migration-dir migration-dir
                :db db
                :modify-sql-fn (if have-pglogical
                                 #(pglogical/wrap-ddl-for-pglogical % pg-schema)
                                 identity)}
        store (mproto/make-store config)]
    (try
      (log/info (i18n/trs "Starting migrations"))
      (mproto/connect store)
      (let [uncompleted-migrations (sort-by mproto/id (migratus/uncompleted-migrations config store))]
        (doseq [migration uncompleted-migrations]
          (let [migration-name (migratus/migration-name migration)
                _ (log/info (i18n/trs "Up {0}" migration-name))
                result (mproto/migrate-up store migration)]
            (when-not (= :success result)
              (log/error (i18n/trs "Failure during migration {0}" migration-name))
              (throw (RuntimeException. ^String (i18n/trs "Failed running migration {0}. Stopping." migration-name))))
            (if (Thread/interrupted)
              (throw (InterruptedException. (i18n/trs "Migrations interrupted because of cancellation.")))))))
      (when have-pglogical
        (pglogical/add-status-alias db pg-schema)
        (pglogical/update-pglogical-replication-set db pg-schema))
      (catch BatchUpdateException e
        (let [root-e (last (seq e))]
          (throw root-e)))
      (finally
        (log/info (i18n/trs "Ending migrations"))
        (mproto/disconnect store)))))

(defn migrate-until-just-before
  "Like 'migrate' but only migrates up to the given
  migration-id (non-inclusive)."
  [db migration-dir migration-id]
  (let [have-pglogical (pglogical/has-pglogical-extension? db)
        pg-schema "public"]
    (try
      (migratus/migrate-until-just-before {:store :database
                                           :migration-dir migration-dir
                                           :db db
                                           :modify-sql-fn (if have-pglogical
                                                            #(pglogical/wrap-ddl-for-pglogical % pg-schema)
                                                            identity)}
                                          migration-id)
      (when have-pglogical
        (pglogical/update-pglogical-replication-set db pg-schema))
      (catch BatchUpdateException e
        (let [root-e (last (seq e))]
          (throw root-e))))))

(defn uncompleted-migrations
  "Returns a list of migrations in migration-dir that haven't run in db"
  [db migration-dir]
  (let [config {:store :database
                :migration-dir migration-dir
                :db db}
        store (mproto/make-store config)]

    (try
      (mproto/connect store)
      (migratus/uncompleted-migrations config store)
      (finally
        (mproto/disconnect store)))))
