(ns puppetlabs.jdbc-util.migration-test
  (:require [puppetlabs.jdbc-util.migration :as migration]
            [clojure.test :refer :all]))

(deftest spec->migration-db-spec-test
  (testing "when migration-user and migration-password aren't specified"
    (let [db-spec {:user "foo" :password "bar"}]
      (is (= db-spec (migration/spec->migration-db-spec db-spec)))))
  (testing "migration-user and migration-password get munged properly"
    (let [db-spec {:user "foo" :password "bar"
                   :migration-user "migrator" :migration-password "awesome"}]
      (is (= {:user "migrator" :password "awesome"}
             (migration/spec->migration-db-spec db-spec))))))
