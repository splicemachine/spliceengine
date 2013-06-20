package com.splicemachine.si.impl;

/**
 * Represents an application level transaction that spans many atomic writes to the underlying data store (i.e. HBase).
 */
public class Transaction extends ImmutableTransaction {
    public static final long ROOT_ID = -1;
    static final Transaction rootTransaction = new Transaction(RootTransactionBehavior.instance, ROOT_ID, 0, 0, null, true,
            false, false, false, TransactionStatus.ACTIVE, null, null, null);

    /**
     * the parent transaction or null if this is a root level transaction
     */
    private final Transaction parent;

    public final TransactionStatus status;

    /**
     * the last time keepAlive was called on this transaction (or when the transaction began). This is an actual timestamp
     * (i.e. number of ms since Jan 1, 1970), not a sequence number.
     */
    public final long keepAlive;

    /**
     * the time when the transaction committed or null if it has not committed
     */
    public final Long commitTimestamp;
    public final Long globalCommitTimestamp;

    public final Long counter;

    public Transaction(TransactionBehavior transactionBehavior, long id, long beginTimestamp, long keepAlive, Transaction parent,
                       boolean dependent, boolean allowWrites, Boolean readUncommitted, Boolean readCommitted,
                       TransactionStatus status, Long commitTimestamp, Long globalCommitTimestamp, Long counter) {
        super(transactionBehavior, id, allowWrites, readCommitted, parent, dependent, readUncommitted,
                beginTimestamp);
        this.keepAlive = keepAlive;
        this.parent = parent;
        this.commitTimestamp = commitTimestamp;
        this.globalCommitTimestamp = globalCommitTimestamp;
        this.status = status;
        this.counter = counter;
    }

    // immediate access

    public Transaction getParent() {
        return parent;
    }

    public Long getCommitTimestampDirect() {
        return commitTimestamp;
    }

    @Override
    public String toString() {
        return "Transaction: " + getTransactionId() + " status: " + status;
    }

    public boolean needsGlobalCommitTimestamp() {
        return behavior.needsGlobalCommitTimestamp();
    }

    // effective functions - these functions walk the transaction ancestry to produce their answer. So they are based
    // not just on the given transaction's state, but also on its ancestors' state.

    /**
     * @return status of this transaction or the status of it's ancestor. Transactions inherit their parent's status
     *         if they don't have one explicitly set for themselves.
     */
    public TransactionStatus getEffectiveStatus() {
        return getEffectiveStatus(rootTransaction);
    }

    public TransactionStatus getEffectiveStatus(ImmutableTransaction scope) {
        if (((status.isActive() || status.isCommitted())
                && !parent.isRootTransaction())
                && !sameTransaction(scope)) {
            return parent.getEffectiveStatus(scope);
        }
        return status;
    }

    public Long getEffectiveCommitTimestamp() {
        return getEffectiveCommitTimestamp(rootTransaction);
    }

    public Long getEffectiveCommitTimestamp(ImmutableTransaction scope) {
        return behavior.getEffectiveCommitTimestamp(scope, globalCommitTimestamp, commitTimestamp, parent);
    }

}
