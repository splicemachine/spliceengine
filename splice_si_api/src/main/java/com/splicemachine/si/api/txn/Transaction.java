package com.splicemachine.si.api.txn;

/**
 *
 *
 */
public interface Transaction {
    /**
     *
     * Unique Transaction Identifier
     *
     * @return
     */
    long getTransactionId();

    /**
     *
     * Unique ID for transaction Parent, or -1L if no parent
     *
     * @return
     */
    long getParentTransactionId();

    /**
     *
     * The timestamp at which this transaction was committted,
     *
     * -1 if rolledback,
     *  0 if in state of committing
     *  -2 if active
     *
     * @return
     */
    long getCommitTimestamp();

    /**
     *
     * The node from where the transaction initiated.  For top level
     * transactions, this will be the node where the JDBC client connected.  For
     * analytical operations, it will be the spark executors node id.
     *
     */
    int getNodeId();

    /**
     *
     * The region from which the transaction was initiated.
     *
     */

    int getRegionId();

    /**
     *
     * The number of milliseconds it took for this specific transactional piece to
     * go from begin to commit/rolledback.
     *
     */
    long getDuration();

    /**
     *
     * The child ids rolled back via the in-memory mechanism.
     *
     */

    long[] getRolledBackChildIds();

    /**
     *
     *
     *
     */
    ChildStatementDuration getChildStatementDuration();

    /**
     *
     *
     *
     */
    long getHLCTimestamp();
    /**
     *
     *
     *
     */
    String getUserId();
    /**
     *
     *
     *
      */
    String getStatementId();
    /**
     *
     *
     *
     */
    TransactionStatus getTransactionStatus();

}
