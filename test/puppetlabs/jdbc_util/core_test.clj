(ns puppetlabs.jdbc-util.core-test
  (:require [clojure.test :refer :all]
            [clojure.walk :refer [keywordize-keys]]
            [puppetlabs.jdbc-util.core :refer :all]))

(deftest submap-aggregation
  (testing "handles nils"
    (is (= '({:a 1 :b 2 :cs {}})
           (->> '({:a 1 :b 2 :c nil :d nil})
                (aggregate-submap-by :c :d :cs)))))
  (testing "nested calls"
    (let [rows [{:value "one-val",
                 :parameter "one",
                 :class "first",
                 :environment "test",
                 :name "group-params"}
                {:value "two-val",
                 :parameter "two",
                 :class "first",
                 :environment "test",
                 :name "group-params"}
                {:value "three-val"
                 :parameter "three"
                 :class "second"
                 :environment "test"
                 :name "group-params"}]]
      (is (= '({:name "group-params"
                :environment "test"
                :classes {:first {:one "one-val"
                                  :two "two-val"}
                          :second {:three "three-val"}}})
             (->> rows
               (aggregate-submap-by :parameter :value :parameters)
               (aggregate-submap-by :class :parameters :classes)
               (keywordize-keys)))))))

(deftest column-aggregation
  (testing "aggregates as expected while preserving order"
    (let [rows [{:white 1 :blue 2 :green 0}
                {:white 1 :blue 4 :green 1}
                {:white 1 :blue 6 :green 2}
                {:white 1 :blue 8 :green 3}
                {:white 1 :blue 10 :green 4}
                {:white 1 :blue 12 :green 5}
                {:white 1 :blue 14 :green 6}
                {:white 1 :blue 16 :green 7}
                {:white 1 :blue 18 :green 8}
                {:white 1 :blue 7 :green 2}
                {:white 1 :blue 19 :green 8}
                {:white 1 :blue 13 :green 5}
                {:white 1 :blue 15 :green 6}
                {:white 1 :blue 5 :green 1}
                {:white 1 :blue 11 :green 4}
                {:white 1 :blue 3 :green 0}
                {:white 1 :blue 17 :green 7}
                {:white 1 :blue 9 :green 3}]]
      (is (= [{:white 1 :blues [2 3] :green 0}
              {:white 1 :blues [4 5] :green 1}
              {:white 1 :blues [6 7] :green 2}
              {:white 1 :blues [8 9] :green 3}
              {:white 1 :blues [10 11] :green 4}
              {:white 1 :blues [12 13] :green 5}
              {:white 1 :blues [14 15] :green 6}
              {:white 1 :blues [16 17] :green 7}
              {:white 1 :blues [18 19] :green 8}]
             (aggregate-column :blue :blues rows))))))

(deftest seq-param-expansion
  (testing "expand-seq-params expands sequential parameters with appropriate
           placeholder while ignoring other parameters"
    (is (= ["SELECT * FROM kerjiggers WHERE version = ? AND name IN (?,?,?) AND manufacturer IN (?,?,?,?)"
            "2.0.0"
            "doohickey" "jobby" "thingie"
            "US Robotics" "Omnicorp" "Cyberdyne Systems" "Morgan Industries"]
           (expand-seq-params
             ["SELECT * FROM kerjiggers WHERE version = ? AND name IN ? AND manufacturer IN ?"
              "2.0.0"
              ["doohickey" "jobby" "thingie"]
              ["US Robotics" "Omnicorp" "Cyberdyne Systems" "Morgan Industries"]])))))
