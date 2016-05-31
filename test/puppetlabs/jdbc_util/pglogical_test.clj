(ns puppetlabs.jdbc-util.pglogical-test
  (:require [clojure.test :refer :all]
            [puppetlabs.jdbc-util.pglogical :refer :all]))

(deftest wrap-ddl-for-pglogical-test
  (is (= (str "do 'begin perform"
              " pglogical.replicate_ddl_command(''set local search_path to public;"
              " create table test(a integer);''"
              "); end;';")
         (wrap-ddl-for-pglogical "create table test(a integer);" "public"))))
