package com.splicemachine.si.impl;

public class RootTransactionBehavior implements TransactionBehavior {
    static RootTransactionBehavior instance = new RootTransactionBehavior();

    @Override
    public Long getGlobalCommitTimestamp(ImmutableTransaction scope, Long globalCommitTimestamp, Long commitTimestamp,
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
