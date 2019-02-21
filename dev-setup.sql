DROP DATABASE if exists jdbc_util_test;
DROP USER if exists jdbc_util_test;

CREATE DATABASE jdbc_util_test;
CREATE USER jdbc_util_test WITH PASSWORD 'foobar' SUPERUSER;