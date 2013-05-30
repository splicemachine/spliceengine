package com.splicemachine.si.impl;

public class IndependentTransactionBehavior implements TransactionBehavior {
    static IndependentTransactionBehavior instance = new IndependentTransactionBehavior();

    @Override
    public Long getGlobalCommitTimestamp(ImmutableTransaction scope, Long globalCommitTimestamp, Long commitTimestamp,
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
