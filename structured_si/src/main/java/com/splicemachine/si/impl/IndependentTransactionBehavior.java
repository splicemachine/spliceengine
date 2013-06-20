package com.splicemachine.si.impl;

/**
 * The behavior plugin for the special kind of nested transactions which commit independently of their parent.
 */
public class IndependentTransactionBehavior implements TransactionBehavior {
    static IndependentTransactionBehavior instance = new IndependentTransactionBehavior();

    @Override
    public Long getEffectiveCommitTimestamp(ImmutableTransaction scope, Long globalCommitTimestamp, Long commitTimestamp,
                                            Transaction parent) {
        return globalCommitTimestamp;
    }

    @Override
    public boolean collapsible() {
        return true;
    }

    @Override
    public boolean needsGlobalCommitTimestamp() {
        return true;
    }
}
