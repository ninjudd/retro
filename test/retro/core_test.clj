(ns retro.core-test
  (:use clojure.test retro.core
        [useful.utils :only [returning]]))

(defn max-rev [rev-seq]
  (dec (count @(:data rev-seq))))

(defrecord RevisionSeq [data queue]
  Queueable
  (enqueue [this f]
    (update-in this [:queue] conj f))
  (get-queue [this]
    queue)
  (empty-queue [this]
    (assoc this :queue []))

  WrappedTransactional
  (txn-wrap [this f]
    (fn [rev-seq]
      (dosync
       (let [rev (current-revision rev-seq)]
         (if (and rev (>= (max-rev rev-seq) rev))
           (f (empty-queue rev-seq)) ;; revision already applied
           (do
             (alter (:data rev-seq)
                    (fn [data]
                      (conj data (peek data))))
             (returning (f rev-seq)
               (alter (:data rev-seq)
                      (fn [data]
                        (let [prev (pop data)]
                          (if (= (peek data) (peek prev))
                            prev, data))))))))))))

(defn get-data [rev-seq]
  (dosync
   (let [rev (or (current-revision rev-seq)
                 (max-rev rev-seq))]
     (nth @(:data rev-seq) rev))))

(defn make [init-data]
  (RevisionSeq. (ref init-data) []))

(defn revisioned-update [rev-seq f & args]
  (modify! rev-seq)
  (apply alter (:data rev-seq)
         update-in [(current-revision rev-seq)] f args))

(defn revisioned-set [rev-seq data]
  (revisioned-update rev-seq (constantly data)))

(defn hard-set [rev-seq data]
  (modify! rev-seq)
  (ref-set (:data rev-seq) data))

(deftest without-revisions
  (let [obj (make [10 20])]
    (dotxn obj
      (enqueue obj #(hard-set % [10 20 30])))
    (is (= 30 (get-data obj)))))

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
