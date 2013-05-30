package com.splicemachine.si.impl;

public class DefaultTransactionBehavior implements TransactionBehavior {
    static DefaultTransactionBehavior instance = new DefaultTransactionBehavior();

    @Override
    public Long getGlobalCommitTimestamp(ImmutableTransaction scope, Long globalCommitTimestamp, Long commitTimestamp,
                                         Transaction parent) {
        if (parent.sameTransaction(scope)) {
            return commitTimestamp;
        } else {
            return parent.getCommitTimestamp(scope);
        }
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
