(ns puppetlabs.jdbc-util.migration
  (:import java.sql.BatchUpdateException)
  (:require [migratus.core :as migratus]
            [migratus.protocols :as mproto]
            [puppetlabs.jdbc-util.pglogical :as pglogical]))

(defn spec->migration-db-spec
  "Given a user defined database config, transform the config into a db-spec
  appropriate for passing to migratus's migrate function."
  [db-config]
  (let [user (or (:migration-user db-config)
                 (:user db-config))
        password (or (:migration-password db-config)
                     (:password db-config))]
    (-> db-config
        (assoc :user user
               :password password)
        (dissoc :migration-user
                :migration-password))))

(defn migrate
  "Migrate 'db' using migratus with a given 'migration-dir'."
  [db migration-dir]
  (let [have-pglogical (pglogical/has-pglogical-extension? db)
        pg-schema "public"]
    (try
      (migratus/migrate {:store :database
                         :migration-dir migration-dir
                         :db db
                         :modify-sql-fn (if have-pglogical
                                          #(pglogical/wrap-ddl-for-pglogical % pg-schema)
                                          identity)})
      (when have-pglogical
        (pglogical/update-pglogical-replication-set db pg-schema))
      (catch BatchUpdateException e
        (let [root-e (last (seq e))]
          (throw root-e))))))

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
