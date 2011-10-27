(ns retro.core
  (:use [useful.utils :only [returning]])
  (:import (javax.transaction InvalidTransactionException TransactionRolledbackException)))

(def ^{:dynamic true} *in-revision* #{})

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

(defprotocol Transactioning
  "An object that is capable of explicitly tracking whether it is in a
   transaction. The behavior of these functions should be unaffected by calls
   to txn-wrap, txn-begin, etc. - retro may, for example, ask you to temporarily
   claim to not be in a transaction, as part of your transaction."
  (in-transaction [obj v]
    "Set whether the object is in a transaction (based on the truthiness
     of v).")
  (in-transaction? [obj]
    "Indicate whether the object is currently in a transaction."))

(defprotocol Queueable
  "A retro object capable of queuing up a list of operations to be performed later,
   typically at transaction-commit time."
  (enqueue [obj f]
    "Add an action - it will be called later with the single argument obj.")
  (get-queue [obj]
    "Return a sequence of this object's pending actions.")
  (empty-queue [obj]
    "Empty this object's queue."))

(defprotocol Revisioned
  (at-revision [obj rev]
    "Return a copy of obj with the current revision set to rev.")
  (current-revision [obj]
    "Return the current revision."))

(let [conj (fnil conj [])]
  (extend-type clojure.lang.IObj
    Queueable
    (enqueue [this f]
      (vary-meta this update-in [::queue] conj f))
    (get-queue [this]
      (-> this meta ::queue))
    (empty-queue [this]
      (vary-meta this assoc ::queue []))

    Revisioned
    (at-revision [this rev]
      (vary-meta this assoc ::revision rev))
    (current-revision [this]
      (-> this meta ::revision))

    Transactioning
    (in-transaction [this v] (vary-meta this assoc ::transaction v))
    (in-transaction? [this] (-> this meta ::transaction))))

(def ^{:dynamic true} *active-transaction* nil)

(defn modify!
  "Alert retro that an operation is about to occur which will modify the given object.
   If there is an active transaction which is not expected to modify the object,
   retro will throw an exception. Should be used similarly to clojure.core/io!."
  [obj]
  (when (and *active-transaction*
             (not (= obj *active-transaction*)))
    (throw (IllegalStateException.
            (format "Attempt to modify %s while in a transaction on %s"
                    obj *active-transaction*)))))

(defn- active-object [f]
  (fn [obj]
    (binding [*active-transaction* obj]
      (f obj))))

(defn- ignore-nested-transactions
  "Takes two functions, one that's wrapped in a transaction and one that's not, returning a new fn
   that calls the transactional fn if not currently in a transaction or otherwise calls the plain fn."
  [f-txn f]
  (fn [obj]
    (if (in-transaction? obj)
      (f obj)
      (in-transaction (f-txn (in-transaction obj true))
                      false))))

(defn- catch-rollbacks
  "Takes a function and wraps it in a new function that catches the exception thrown by abort-transaction."
  [f]
  (fn [obj]
    (try (f obj)
         (catch TransactionRolledbackException e obj))))

(defn wrapped-txn [f obj]
  (if (satisfies? WrappedTransactional obj)
    (txn-wrap obj f)
    (fn [obj]
      (txn-begin obj)
      (try (returning (f obj)
             (txn-commit obj))
           (catch Throwable e
             (txn-rollback obj)
             (throw e))))))

(defn wrap-transaction
  "Takes a function and returns a new function wrapped in a transaction on the given object."
  [f obj]
  (-> (active-object f)
      (wrapped-txn obj)
      (catch-rollbacks)
      (ignore-nested-transactions f)))

(defmacro with-transaction
  "Execute forms within a transaction on the specified object."
  [obj & forms]
  `(let [obj# ~obj]
     ((wrap-transaction (fn [inner-obj#]
                          (do ~@forms
                              inner-obj#))
                        obj#)
      obj#)))

(defn abort-transaction
  "Throws an exception that will be caught by catch-rollbacks to abort the transaction."
  []
  (throw (TransactionRolledbackException.)))

(defmacro dotxn
  "Perform body in a transaction around obj. The body should evaluate to a version of
   obj with some actions enqueued via its implementation of Queueable; those actions
   will be performed after the object has been passed through its before-mutate hook."
  [obj & body]
  `((wrap-transaction (fn [new-obj#]
                        (doseq [f# (get-queue new-obj#)]
                          (f# new-obj#))
                        (empty-queue new-obj#))
                      ~obj)
    (binding [*active-transaction* 'writes-disabled]
      ~@body)))
