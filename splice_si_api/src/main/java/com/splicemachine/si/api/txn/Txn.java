package com.splicemachine.si.api.txn;

import java.io.Externalizable;

/**
 *
 *
 */
public interface Txn extends Comparable<Txn>, Externalizable{
    public static final int ACTIVE = -2;
    public static final int ROLLEDBACK = -1;
    public static final int COMMITTING = 0;

    /**
     *
     * Unique Txn Identifier
     *
     * @return
     */
    long getTxnId();

    /**
     *
     * Unique Txn Identifier
     *
     * @return
     */
    void setTxnId(long txnId);

    /**
     *
     * Unique ID for transaction Parent, or -1L if no parent
     *
     * @return
     */
    long getParentTxnId();

    /**
     *
     * Unique ID for transaction Parent, or -1L if no parent
     *
     * @return
     */
    void setParentTxnId(long parentTxnId);
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
     * The timestamp at which this transaction was committted,
     *
     * -1 if rolledback,
     *  0 if in state of committing
     *  -2 if active
     *
     * @return
     */
    void setCommitTimestamp(long commitTimestamp);

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
     * The node from where the transaction initiated.  For top level
     * transactions, this will be the node where the JDBC client connected.  For
     * analytical operations, it will be the spark executors node id.
     *
     */
    void setNodeId(int nodeId);
    /**
     *
     * The region from which the transaction was initiated.
     *
     */
    int getRegionId();
    /**
     *
     * The region from which the transaction was initiated.
     *
     */
    void setRegionId(int regionId);
    /**
     *
     * The number of milliseconds it took for this specific transactional piece to
     * go from begin to commit/rolledback.
     *
     */
    long getDuration();
    /**
     *
     * The number of milliseconds it took for this specific transactional piece to
     * go from begin to commit/rolledback.
     *
     */
    void setDuration(long duration);

    /**
     *
     * The child ids rolled back via the in-memory mechanism.
     *
     */

    long[] getRolledBackChildIds();
    /**
     *
     * The child ids rolled back via the in-memory mechanism.
     *
     */

    void setRolledBackChildIds(long[] rolledBackChildIds);
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
    void setChildStatementDuration(ChildStatementDuration childStatementDuration);
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
    void setHLCTimestamp(long hlcTimestamp);
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
    void setUserId(String userId);
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
    void setStatementId(String statementId);
    /**
     *
     *
     *
     */
    TransactionStatus getTransactionStatus();
    /**
     *
     *
     */
    boolean isPersisted();
    /**
     *
     *
     */
    void persist();

    boolean isRolledback();

    boolean isCommitted();

    boolean isCommitting();

    boolean isActive();

    boolean isAbleToCommit();
}
