(ns puppetlabs.jdbc-util.pglogical-test
  (:require [clojure.test :refer :all]
            [puppetlabs.jdbc-util.pglogical :refer :all]))

(deftest wrap-ddl-for-pglogical-test
  (is (= (str "do 'begin perform"
              " pglogical.replicate_ddl_command(''set local search_path to public;"
              " create table test(a integer);''"
              "); end;';")
         (wrap-ddl-for-pglogical "create table test(a integer);" "public"))))

(deftest replication-status-test
  (testing "when 2 subscriptions are running, returns :running"
    (is (= :running (consolidate-replication-status ["replicating" "replicating"]))))
  (testing "when one subscription is down,"
    (testing "and the rest are running, returns :down"
      (is (= :down (consolidate-replication-status ["replicating" "down"]))))
    (testing "and another is disabled, returns :down"
      (is (= :down (consolidate-replication-status ["disabled" "down"])))))
  (testing "when one subscription is disabled,"
    (testing "and the rest are running, returns :disabled"
      (is (= :disabled (consolidate-replication-status ["replicating" "disabled"])))))
  (testing "when no subscriptions are configured, returns :disabled"
    (testing "and the rest are running, returns :disabled"
      (is (= :disabled (consolidate-replication-status []))))))
