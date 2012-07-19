(ns retro.core-test
  (:use clojure.test retro.core
        useful.debug
        [useful.utils :only [returning]]))

(defrecord RevisionMap [revisions committed-revisions]
  Transactional
  (txn-begin! [this]
    (reset! committed-revisions @revisions))
  (txn-commit! [this]
    nil)
  (txn-rollback! [this]
    (reset! revisions @committed-revisions))

  OrderedRevisions
  (max-revision [this]
    (first (keys @revisions)))
  (touch [this]
    (swap! revisions
           (fn [revisions]
             (let [curr (current-revision this)]
               (if (contains? revisions curr)
                 revisions
                 (assoc revisions curr (first (vals revisions)))))))))

(defn data-at [revision-map revision]
  (first (vals (subseq @(:revisions revision-map)
                       >= revision))))

(defn active-revision [revision-map]
  (some #(% revision-map) [current-revision max-revision]))

(defn current-data [revision-map]
  (data-at revision-map (active-revision revision-map)))

(defn update-data [revision-map f]
  (swap! (:revisions revision-map)
         (fn [revisions]
           (assoc revisions (current-revision revision-map)
                  (f (current-data revision-map))))))

(defn make [init-data]
  (let [m (into (sorted-map-by >) init-data)]
    (RevisionMap. (atom m) (atom m))))

(def ^:dynamic a)
(def ^:dynamic b)

(use-fixtures :each (fn [f]
                      (binding [a (make {1 1})
                                b (make {1 1 2 2})]
                        (f))))

(deftest test-revisionmap
  (is (= 1 (current-data a)))
  (is (= 2 (current-data b)))
  (is (= 1 (current-data (at-revision b 1)))))

(deftest test-dotxn
  (let [a (at-revision a 3)
        b (at-revision b 3)]
    (dotxn [a b]
      (update-data a inc)
      (update-data b inc)))

  (is (= 3 (max-revision a) (max-revision b)))
  (is (= 2 (current-data a)))
  (is (= 3 (current-data b))))

(deftest test-txn
  (let [a (at-revision a 2)
        b (at-revision b 2)]
    (txn [a b]
      {a [#(update-data % inc) #(update-data % inc)]
       b [#(update-data % (partial + 10))]}))

  (is (= 3 (max-revision a) (max-revision b)))
  (is (= 3 (current-data a)))
  (is (= 12 (current-data b))))

(deftest test-applied-revisions
  (let [a (at-revision a 1)
        b (at-revision b 1)]
    (txn [a b]
      {a [#(update-data % inc)] ;; should apply, because a has no revision 2
       b [#(update-data % inc)]})) ;; should be skipped: b has already seen revision 2

  (is (= 2 (max-revision a) (max-revision b)))
  (is (= 2 (current-data a)))
  (is (= 2 (current-data b))))

(comment

 (deftest with-revisions
   (let [obj (at-revision (make [10 20]) 1)]
     (is (= 20 (get-data obj)))
     (is (= 10 (get-data (at-revision obj 0))))
     (dotxn obj
       (enqueue (at-revision obj 2) #(revisioned-set % 30)))
     (is (= 20 (get-data obj)))
     (is (= 30 (get-data (at-revision obj 2))))
     (is (= 30 (get-data (at-revision obj nil))))))

 (deftest skip-old-revisions
   (let [obj (at-revision (make [10 20 30]) 1)]
     (is (= 20 (get-data obj)))
     (dotxn obj
       (enqueue obj #(revisioned-set % 100)))
     (testing "changing the past is a NOP"
       (is (= 20 (get-data obj)))
       (is (= 2 (max-rev obj))))))

 (deftest test-visibility
   (let [obj (at-revision (make [10 20]) 1)]
     (dotxn obj
       (let [obj (enqueue (at-revision obj 2) #(revisioned-set % 30))]
         (testing "can't see pending revisions"
           (is (thrown? Exception ;; shouldn't actually be written yet
                        (get-data obj))))
         (enqueue obj #(revisioned-update % inc))))
     (testing "updates are applied in sequence"
       (is (= 31 (get-data (at-revision obj nil)))))))

 (deftest test-transactionality
   (let [obj (at-revision (make [10 20]) 1)]
     (testing "exceptions propagate"
       (is (thrown? Exception
                    (dotxn obj
                      (-> obj (at-revision 2)
                          (enqueue #(revisioned-set % 30))
                          (enqueue #(inc "TEST")))))))
     (testing "exceptions cause writes to be cancelled"
       (is (= 20 (get-data (at-revision obj nil)))))))

 (deftest test-no-mutators
   (let [obj1 (make [10 20])
         obj2 (make '[a b])]
     (testing "in a transaction you can only enqueue actions"
       (is (thrown? Exception
                    (dotxn obj1
                      (hard-set obj1 [10 20 30])))))
     (testing "can't mutate x while applying queued actions for y"
       (is (thrown? Exception
                    (dotxn obj1
                      (dotxn obj2
                        (enqueue obj2 (fn [_]
                                        (hard-set obj1 [10 20 30]))))
                      obj1))))))
)