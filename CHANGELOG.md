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
