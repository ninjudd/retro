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
                 (assoc revisions curr (first (vals revisions))))))))

  Object
  (toString [this] (pr-str this)))

(defn data-at [revision-map revision]
  (first (vals (subseq @(:revisions revision-map)
                       >= revision))))

(defn active-revision [revision-map]
  (some #(% revision-map) [current-revision max-revision]))

(defn current-data [revision-map]
  (data-at revision-map (active-revision revision-map)))

(defn update-data [revision-map f]
  (modify! revision-map)
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
        b (at-revision b 2)
        txn-result (txn [a b]
                     (with-actions 'x
                       {a [#(update-data % inc) #(update-data % inc)]
                        b [#(update-data % (partial + 10))]}))]
    (is (= 'x (:value txn-result)))
    (is (empty? (:actions txn-result))))

  (is (= 3 (max-revision a) (max-revision b)))
  (is (= 3 (current-data a)))
  (is (= 12 (current-data b))))

(deftest test-nested-txn
  (let [a (at-revision a 2)
        b (at-revision b 2)
        io (with-actions 'value1
             {a [#(update-data % inc)]
              b [#(update-data % (constantly 10))]})
        txn1-result (txn [a] io)]
    (is (= 'value1 (:value txn1-result)))
    (is (= #{b} (set (keys (:actions txn1-result)))))

    (is (= 3 (max-revision a)))
    (is (= 1 (current-data a))) ;; revision-2 view should be unsullied
    (is (= 2 (current-data (at-revision a 3)))) ;; but 3 should be committed

    (is (= 2 (max-revision b))) ;; b should be entirely untouched
    (is (= 2 (current-data b)))
    (is (= 2 (current-data (at-revision b 3))))

    (let [txn2-result (txn [b] (with-actions 'value2 (:actions txn1-result)))]
      (is (= 'value2 (:value txn2-result)))
      (is (empty? (:actions txn2-result)))

      ;; same tests on a as above: it should not be touched at all by this txn
      (is (= 3 (max-revision a)))
      (is (= 1 (current-data a)))
      (is (= 2 (current-data (at-revision a 3))))

      ;; but now b should have caught up
      (is (= 3 (max-revision b)))
      (is (= 2 (current-data b)))
      (is (= 10 (current-data (at-revision b 3)))))))

(deftest test-applied-revisions
  (let [a (at-revision a 1)
        b (at-revision b 1)]
    (is (= 'x)
        (txn [a b]
          (with-actions 'x
            {a [#(update-data % inc)] ;; should apply, because a has no revision 2
             b [#(update-data % inc)]})))) ;; should be skipped: b has already seen revision 2

  (is (= 2 (max-revision a) (max-revision b)))
  (is (= 2 (current-data a)))
  (is (= 2 (current-data b))))
