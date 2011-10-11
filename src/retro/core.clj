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

(defprotocol Queueable
  "A retro object capable of queuing up a list of operations to be performed later,
   typically at transaction-commit time."
  (enqueue [obj f]
    "Add an action - it will be called later with the single argument obj.")
  (get-queue [obj]
    "Return a sequence of this object's pending actions."))

(defprotocol Revisioned
  (at-revision [obj rev]
    "Return a copy of obj with the current revision set to rev.")
  (current-revision [obj]
    "Return the current revision."))

(defprotocol TransactionHooks
  (before-mutate [obj]
    "Called before beginning to apply queued mutations on obj. The return value of
     this hook is used instead of obj, so you may (for example) empty the queue to
     cancel all pending actions."))

(let [conj (fnil conj [])]
  (extend-type IObj
    Queueable
    (enqueue [this f]
      (vary-meta this update-in [::queue] conj f))
    (get-queue [this]
      (-> this meta ::queue))

    Revisioned
    (at-revision [this rev]
      (vary-meta this assoc ::revision rev))
    (current-revision [this]
      (-> this meta ::revision))))

(extend-protocol TransactionHooks
  Object
  (before-mutate [obj]
    obj))

(def ^:dynamic *active-transaction* nil)

(defn modify!
  "Alert retro that an operation is about to occur which will modify the given object.
   If there is an active transaction which is not expected to modify the object,
   retro will throw an exception. Should be used similarly to clojure.core/io!."
  [obj]
  (when (and *active-transaction*
             (not (identical? obj *active-transaction*)))
    (throw (IllegalStateException.
            (format "Attempt to modify %s while in a transaction on %s"
                    obj *active-transaction*)))))

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

(defmacro dotxn
  "Perform body in a transaction around obj. The body should evaluate to a version of
   obj with some actions enqueued via its implementation of Queueable; those actions
   will be performed after the object has been passed through its before-mutate hook."
  [obj & body]
  `(with-transaction ~obj
     (binding [*active-transaction* 'writes-disabled] ;; no bang methods allowed anywhere
       (let [obj# (before-mutate (do ~@body))
             queue# (get-queue obj#)]
         (binding [*active-transaction* obj#] ;; bang methods on this object allowed now
           (doseq [f# queue#]
             (f# obj#)))))))
