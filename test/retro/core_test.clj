(ns retro.core-test
  (:use clojure.test retro.core))

(defn max-rev [rev-seq]
  (dec (count @(:data rev-seq))))

(defrecord RevisionSeq [data queue]
  Queueable
  (enqueue [this f]
    (update-in this [:queue] conj f))
  (get-queue [this]
    queue)

  TransactionHooks
  (before-mutate [this]
    (let [rev (current-revision this)]
      (if (and rev (> (max-rev this) rev))
        (assoc this :queue []) ;; revision already applied, dump queue
        this)))

  WrappedTransactional
  (txn-wrap [this f]
    #(dosync (f))))

(defn get-data [rev-seq]
  (let [rev (or (current-revision rev-seq)
                (max-rev rev-seq))]
    (nth @(:data rev-seq) rev)))

(defn make [init-data]
  (RevisionSeq. (ref init-data) []))

(defn revisioned-update [rev-seq f & args]
  (modify! rev-seq)
  (apply alter (:data rev-seq)
         update-in [(inc (current-revision rev-seq))] f args))

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
      (enqueue obj #(revisioned-set % 30)))
    (is (= 20 (get-data obj)))
    (is (= 30 (get-data (at-revision obj 2))))
    (is (= 30 (get-data (at-revision obj nil))))))

(deftest skip-old-revisions
  (let [obj (at-revision (make [10 20 30]) 1)]
    (is (= 20 (get-data obj)))
    (dotxn obj
      (enqueue obj #(revisioned-set % 100)))
    (is (= 20 (get-data obj)))
    (is (= 2 (max-rev obj)))))

(deftest test-visibility
  (let [obj (at-revision (make [10 20]) 1)]
    (dotxn obj
      (let [obj (enqueue obj #(revisioned-set % 30))]
        (is (thrown? Exception ;; shouldn't actually be written yet
                     (get-data (at-revision obj 2))))
        (enqueue obj #(revisioned-update % inc))))
    (is (= 31 (get-data (at-revision obj nil))))))

(deftest test-transactionality
  (let [obj (at-revision (make [10 20]) 1)]
    (is (thrown? Exception
                 (dotxn obj
                   (-> obj
                       (enqueue #(revisioned-set % 30))
                       (enqueue #(inc "TEST"))))))
    (is (= 20 (get-data (at-revision obj nil)))) ;; nothing persisted
    ))

(deftest test-no-mutators
  (let [obj1 (make [10 20])
        obj2 (make '[a b])]
    (is (thrown? Exception
                 (dotxn obj1
                   (hard-set obj1 [10 20 30]))))
    (is (thrown? Exception
                 (dotxn obj1
                   (dotxn obj2
                     (enqueue obj2 (fn [_]
                                     (hard-set obj1 [10 20 30]))))
                   obj1)))))