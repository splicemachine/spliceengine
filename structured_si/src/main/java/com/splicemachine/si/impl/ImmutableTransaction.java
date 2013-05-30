package com.splicemachine.si.impl;

/**
 * Represents the parts of a transaction record that do not change once the transaction is created. It is useful to
 * represent these immutable parts separately because they can be cached more aggressively.
 */
public class ImmutableTransaction {
    private final ImmutableTransactionRelativeState state;
    private final SITransactionId transactionId;
    protected final Boolean dependent;

    private final ImmutableTransaction immutableParent;
    private final boolean allowWrites;

    public ImmutableTransaction(Boolean dependent, boolean allowWrites, Boolean readCommitted,
                                ImmutableTransaction immutableParent, Boolean readUncommitted, long beginTimestamp) {
        this.state = new ImmutableTransactionRelativeState(beginTimestamp, readCommitted, readUncommitted);
        this.dependent = dependent;
        this.allowWrites = allowWrites;
        this.immutableParent = immutableParent;
        this.transactionId = new SITransactionId(beginTimestamp);
    }

    public long getBeginTimestamp() {
        return state.beginTimestamp;
    }

    public SITransactionId getTransactionId() {
        return transactionId;
    }

    /**
     * Returns true if this is _not_ a root transaction.
     */
    boolean isNested() {
        return immutableParent != null;
    }

    /**
     * Returns true if this is a child transaction that is dependent on its parent. Dependent transactions do not
     * commit independently (i.e. they don't finally commit until the parent does).
     */
    public boolean isNestedDependent() {
        return isNested() && dependent;
    }

    public boolean getEffectiveReadUncommitted() {
        if (state.readUncommitted == null) {
            return immutableParent.getEffectiveReadUncommitted();
        }
        return state.readUncommitted;
    }

    public boolean getEffectiveReadCommitted() {
        if (state.readCommitted == null) {
            return immutableParent.getEffectiveReadCommitted();
        }
        return state.readCommitted;
    }

    public long getRootBeginTimestamp() {
        if (immutableParent == null) {
            return state.beginTimestamp;
        }
        return immutableParent.getRootBeginTimestamp();
    }

    public boolean isReadOnly() {
        return !allowWrites;
    }

}
