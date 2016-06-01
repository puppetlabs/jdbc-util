## 0.4.1

 * Add a `middleware` namespace with ring middleware for catching Postgres permission errors and returning an http response.
 * Add utility functions for pglogical support under the `pglogical` namespace.
 * Add the `puppetlabs/i18n` library and externalize strings.

## 0.4.0

 * Update `java.jdbc` dependency to 0.6.1. This version of `java.jdbc` has breaking changes.

## 0.3.1

 * Convert PGObjects to their textual value in `core/query` - It is expected that users who don't want this functionality will either call `jdbc/query` directly or implement their own wrapper using the component functions called by `core/query`
