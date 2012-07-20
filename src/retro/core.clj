(ns retro.core
  (:use [useful.utils :only [returning]]))

(defprotocol Transactional
  (txn-begin! [obj]
    "Begin a new transaction.")
  (txn-commit! [obj]
    "Commit the current transaction.")
  (txn-rollback! [obj]
    "Roll back the current transaction."))

(defprotocol WrappedTransactional
  (txn-wrap [obj f]
    "Wrap the given function in a transaction, returning a new function."))

(defprotocol Revisioned
  (at-revision [obj rev]
    "Return a copy of obj with the current revision set to rev.")
  (current-revision [obj]
    "Return the current revision."))

(defprotocol Applied
  (revision-applied? [obj rev]
    "Tell whether the revision named by rev has already been written."))

(defprotocol OrderedRevisions
  (max-revision [obj]
    "What is the 'latest' revision that has been applied? Should be unaffected by at-revision
     'views'. nil is an acceptable answer, meaning 'none', or 'I'm not tracking that'.")
  (touch [obj]
    "Mark the current revision as being applied, guaranteeing that max-revision returns a
     number at least as large as the object's current revision."))

(let [conj (fnil conj [])]
  (extend-type clojure.lang.IObj
    Revisioned
    (at-revision [this rev]
      (vary-meta this assoc ::revision rev))
    (current-revision [this]
      (-> this meta ::revision))
    (revision-applied? [this rev]
      false)))

(extend-type Object
  WrappedTransactional
  (txn-wrap [this f]
    (fn []
      (txn-begin! this)
      (try (returning (f)
             (txn-commit! this))
           (catch Throwable e
             (txn-rollback! this)
             (throw e)))))
  Applied
  (revision-applied? [this rev]
    (when-let [max (max-revision this)]
      (>= max rev)))

  OrderedRevisions
  (max-revision [this]
    nil)
  (touch [this]
    nil))

(def ^{:dynamic true} *read-only* nil)

(defn modify!
  "Alert retro that an operation is about to occur which will modify the given object.
   If there is an active transaction which is not expected to modify the object,
   retro will throw an exception. Should be used similarly to clojure.core/io!."
  [obj]
  (when *read-only*
    (throw (IllegalStateException.
            (format "Attempt to modify %s while in read-only mode" obj)))))

(do
  ;;; These function-wrapping functions behave kinda like ring wrappers: they
  ;;; return a function which takes a retro-object and returns a retro-object.
  (defn wrap-touching
    "Wrap a function so that the active object is touched at the end."
    [obj f]
    (fn []
      (returning (f)
        (touch obj))))

  (defn wrap-transaction
    "Takes a function and returns a new function wrapped in a transaction on the given object."
    [obj f]
    (->> f
         (wrap-touching obj)
         (txn-wrap obj))))

(defn- call-wrapped*
  "Calls [f] inside a transaction on all the [objects]. Assumes they all implement Transactional,
  so that it is acceptable to call txn-begin! on them all, apply f, and then txn-commit! them all
  in reverse order."
  [objects f]
  (doseq [obj objects]
    (txn-begin! obj))
  (let [apply! (fn [& fs]
                 (doseq [obj (rseq objects), f fs]
                   (f obj)))]
    (try
      (f)
      (apply! touch txn-commit!)
      (catch Throwable t
        (apply! txn-rollback!)
        (throw t)))))

(defn call-wrapped
  "Calls [f] inside a transaction on all the [objects]. Attempts to minimize stack depth by using
  Transactional when possible, but will use WrappedTransactional as necessary."
  [objects f]
  (loop [f f, to-wrap [], objects objects]
    (if-let [[x & xs] (seq objects)]
      (if (satisfies? Transactional x)
        (recur f
               (conj to-wrap x)
               xs)
        (let [wrapped-f (wrap-transaction x f)]
          (recur #(call-wrapped* to-wrap wrapped-f)
                 []
                 xs)))
      (call-wrapped* to-wrap f))))

(defn txn* [foci action-thunk]
  (let [actions (binding [*read-only* true] (action-thunk))]
    (reduce (fn [actions focus]
              (let [read-revision (current-revision focus)
                    write-revision (if read-revision
                                     (inc read-revision)
                                     read-revision)]
                (when-not (and write-revision
                               (revision-applied? focus write-revision))
                  (let [write-view (if write-revision
                                     (at-revision focus write-revision)
                                     focus)]
                    (binding [*read-only* false]
                      (call-wrapped [write-view]
                                    (fn []
                                      (doseq [action (get actions focus)]
                                        (action write-view))))))))
              (dissoc actions focus))
            actions
            foci)))

(defmacro txn [foci actions]
  `(txn* ~foci (fn [] ~actions)))

(defmacro dotxn [foci & body]
  `(call-wrapped ~foci (fn [] ~@body)))
