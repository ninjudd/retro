(ns retro.core
  (:import (javax.transaction InvalidTransactionException TransactionRolledbackException)))

(def ^{:dynamic true} *in-transaction* #{})
(def ^{:dynamic true} *in-revision*    #{})

(defprotocol Transactional
  (txn-begin [obj]
    "Begin a new transaction.")
  (txn-commit [obj]
    "Commit the current transaction.")
  (txn-rollback [obj]
    "Rollback the current transaction."))

(defprotocol WrappedTransactional
  (txn-wrap [obj f]
    "Wrap the given function in a transaction, returning a new function."))

(defprotocol Revisioned
  (get-revisions [obj key]
    "Returns a list of at least one previous revision if any exist.")
  (at-revision [obj rev]
    "Return a copy of obj with the current revision set to rev.")
  (current-revision [obj]
    "Return the current revision."))

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
         (catch TransactionRolledbackException e))))

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
      (ignore-nested-transactions f obj)))

(defmacro with-transaction
  "Execute forms within a transaction on the specified object."
  [obj & forms]
  `((wrap-transaction (fn [] ~@forms) ~obj)))

(defn abort-transaction
  "Throws an exception that will be caught by catch-rollbacks to abort the transaction."
  []
  (throw (TransactionRolledbackException.)))
