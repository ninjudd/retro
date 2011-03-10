(ns retro.core)

(def ^{:dynamic true} *in-transaction* #{})
(def ^{:dynamic true} *in-revision*    #{})
(def ^{:dynamic true} *revision*       nil)

(defprotocol Transactional
  (txn-begin    [obj] "Begin a new transaction.")
  (txn-commit   [obj] "Commit the current transaction.")
  (txn-rollback [obj] "Rollback the current transaction."))

(defprotocol WrappedTransactional
  (txn-wrap [obj f] "Wrap the given function in a transaction, returning a new function."))

(defprotocol Revisioned
  (get-revision  [obj]     "Returns the current revision.")
  (set-revision! [obj rev] "Set the current revision to rev."))

(defmacro at-revision
  "Execute the given forms with the curren revision set to rev. Can be used to mark changes with a given
   revision, or read the state at a given revision."
  [rev & forms]
  `(binding [*revision* ~rev]
     ~@forms))

(defn- skip-past-revisions
  "Wraps the given function in a new function that skips it if the current revision has already
   been applied, also setting the revision upon executing the function."
  [f obj]
  (fn []
    (if (or (nil? *revision*) (contains? *in-revision* obj))
      (f)
      (let [rev (or (get-revision obj) 0)]
        (if (<= *revision* rev)
          (printf "skipping revision: revision [%s] <= current revision [%s]\n" *revision* rev)
          (binding [*in-revision* (conj *in-revision* obj)]
            (let [result (f)]
              (if *revision*
                (set-revision! obj *revision*))
              result)))))))

(defn- ignore-nested-transactions
  "Takes two functions, one that's wrapped in a transaction and one that's not, returning a new fn
   that calls the transactional fn if not currently in a transaction or otherwise calls the plain fn."
  [f-txn f obj]
  (fn []
    (if (contains? *in-transaction* obj)
      (f)
      (binding [*in-transaction* (conj *in-transaction* obj)]
        (f-txn)))))

(defn- catch-rollbacks
  "Takes a function and wraps it in a new function that catches the exception thrown by abort-transaction."
  [f]
  (fn []
    (try (f)
         (catch javax.transaction.TransactionRolledbackException e))))

(defn wrapped-txn [f obj]
  (if (satisfies? WrappedTransactional obj)
    (txn-wrap obj f)
    (fn []
      (txn-begin obj)
      (try (let [result (f)]
             (txn-commit obj)
             result)
           (catch Throwable e
             (txn-rollback obj)
             (throw e))))))

(defn wrap-transaction
  "Takes a function and returns a new function wrapped in a transaction on the given object."
  [f obj]
  (-> (wrapped-txn f obj)
      (catch-rollbacks)
      (ignore-nested-transactions f obj)
      (skip-past-revisions obj)))

(defmacro with-transaction
  "Execute forms within a transaction on the specified object."
  [obj & forms]
  `((wrap-transaction (fn [] ~@forms) ~obj)))

(defn abort-transaction
  "Throws an exception that will be caught by catch-rollbacks to abort the transaction."
  []
  (throw (javax.transaction.TransactionRolledbackException.)))
