## 1.2.5
  * alter the `uncompleted-migrations` function to not have any side-effects.  Prior to this change, the routine would create the migraiton table if it didn't exist. 

## 1.2.4 
  * utility methods for transforming data to PGObject jsonb format and reversing the transformation

## 1.2.3
  * wait until migrations have reached a safe point before closing the pool.  If the pool is closed during a migration step,
  the migration is marked as reserved and won't be able to continue.

## 1.2.2
  * update to migratus 1.0.8
  * change migration function to use interruptibility built into migratus.

## 1.2.1
  * alter migration function to observe interruptibility of the thread so that migrations can be interrupted
  * cleanup connection used in `uncompleted-migrations` function to prevent leak.

## 1.2.0
  * alter pool initialization behavior to retry transients, but fail on everything else.
  * alter pool getConnection to throw a RuntimeException when trying to use the pool with an initialization failure
  * alter pool behavior to run migrations when replication-mode is left out of the migration options
  * multiple test flakeyness issues resolved

## 1.1.1
  * fix issue with pg-logical wrappers and multiple statements issued by migratus

## 1.1.0
  * Update libraries for Java 9 compatibility
  * update migratus to version 1.0.3
  * update postgresql driver to 42.2.0
  * update java.jdbc to 0.7.5

## 1.0.3
  * update HikariCP to version 2.7.4
  * update postgresql driver to 42.1.4

## 1.0.2
  * fix deprecation warning on 'initializationFailFast' in Hikari configuration

## 1.0.1
  * fix an issue where exceptions during migration for a replica were not caught

## 1.0.0
  * update HikariCP to version 2.6.1
  * update kitchensink to 0.3.0
  * update cheshire to 5.7.1
  * update dev dependency slf4j related items to 1.7.22 to sync with Hikari

## 0.6.2
  * Fix `create-db!` when the owner of the new database is a different user than
    the one creating the database.
  * Add `has-role?`.

## 0.6.1
  * Improved Japanese translation strings.

## 0.6.0
  * Add `db-exists?`, `create-db!`, and `drop-db!`.
  * Add `user-exists?`, `create-user!`, and `drop-user!`.

## 0.5.0
  * Add lifetime protocol to pool interface to allow shutdown routines to wait
    for initialization to complete.

## 0.4.15

  * Fix `consolidate-provider-status` to correctly report the status.

## 0.4.14

  * Fix an issue where the Hikari config option `setInitializationFailFast` was
    set after the connection was made, which meant that the option wasn't applied
    to the connection.

## 0.4.13

  * Change the behavior of `reconcile-sequence-for-column!` so that it will
    never set a sequence to a lower value than it currently has.
  * Update the migratus dependency to 0.8.30 (no breaking changes).

## 0.4.12

 * Remove the `NOWAIT` argument when locking a table for
   `reconcile-sequence-for-column!`. This is unlikely to do anything but cause
   problems in most circumstances.

## 0.4.11

 * Fixed a bug with `spec->migration-db-spec` that would allow nil `:password`
   keys to appear in the output. The same function now also properly separates
   `:user` from `:migration-password` and `:migration-user` from `:password`,
   so that the two sets of credentials won't be erroneously combined.

## 0.4.10

 * Added the ability to pass in a timeout to `combined-replication-status`
   (defaulting to four seconds) which will cause the replication status to be
   unknown if the replication status cannot be retrieved in the span of the
   timeout. This fixed a bug with `replication-status` which caused
   trapperkeeper-status checks to timeout when no database connection was
   available because the replication status check was not timing out.

## 0.4.9

 * Fix a bug with `reconcile-sequence-for-column!` that prevented it from
   working when the given column was empty.

## 0.4.8

 * Block startup of `:replication-mode` replicas waiting on migrations. If the
   application is configured as a replica wait on migrations before initializing
   the database (useful for waiting on pglogical replication which will
   replicate the migrations).
 * Added `Add reconcile-sequence-for-column!` that will reset a sequence to the
   maximum value in the column, or else 0.

## 0.4.7

 * Add support for running migrations when creating a connection pool (separate from the init-fn).
 * Add functionality for retrieving the status of pglogical replication.
 * Add `drop-public-functions!` utility function.

## 0.4.6

 * Allow the connection health check timeout to be set via `options->hikari-config` using the :connection-check-timeout entry.
 * Select the :connection-check-timeout entry in `select-user-configurable-hikari-options`.

## 0.4.5

 * Change exception behavior of `update-pglogical-replication-set`. If the db user doesn't have rights to update pglogical, catches the exception and returns false. Other exceptions are passed through.

## 0.4.4

 * Add `select-user-configurable-hikari-options` to provide a consistent way of preserving only the hikari specific options from a given map of options.

## 0.4.3

 * Specifying the timeout in `wrap-with-delayed-init` does not allow the connectivity timeout to be configured independently. Added `add-connectivity-check-timeout-ms` to allow independent configuration.

## 0.4.2

 * Change the `handle-postgres-permission-errors` to generate a 500 code instead of a 403 when database permissions fail.

## 0.4.1

 * Add a `middleware` namespace with ring middleware for catching Postgres permission errors and returning an http response.
 * Add utility functions for pglogical support under the `pglogical` namespace.
 * Add the `puppetlabs/i18n` library and externalize strings.

## 0.4.0

 * Update `java.jdbc` dependency to 0.6.1. This version of `java.jdbc` has breaking changes.

## 0.3.1

 * Convert PGObjects to their textual value in `core/query` - It is expected that users who don't want this functionality will either call `jdbc/query` directly or implement their own wrapper using the component functions called by `core/query`
