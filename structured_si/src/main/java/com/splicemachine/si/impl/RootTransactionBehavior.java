package com.splicemachine.si.impl;

/**
 * Defines the behavior for the special transaction that is considered as the parent of all otherwise top-level transactions.
 */
public class RootTransactionBehavior implements TransactionBehavior {
    static RootTransactionBehavior instance = new RootTransactionBehavior();

    @Override
    public Long getEffectiveCommitTimestamp(ImmutableTransaction scope, Long globalCommitTimestamp, Long commitTimestamp,
                                            Transaction parent) {
        return null;
    }

    @Override
    public boolean collapsible() {
        return false;
    }

    @Override
    public boolean needsGlobalCommitTimestamp() {
        return false;
    }
}
