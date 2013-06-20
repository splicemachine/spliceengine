package com.splicemachine.si.impl;

/**
 * Defines the behavior of the trivial transaction objects used to represent committed or failed transactions constructed
 * from the commit timestamps on data rows (these only contain begin/end timestamps and status).
 */
public class StubTransactionBehavior implements TransactionBehavior {
    static StubTransactionBehavior instance = new StubTransactionBehavior();

    @Override
    public Long getEffectiveCommitTimestamp(ImmutableTransaction scope, Long globalCommitTimestamp, Long commitTimestamp,
                                            Transaction parent) {
        if (parent.sameTransaction(scope)) {
            return commitTimestamp;
        } else {
            return parent.getEffectiveCommitTimestamp(scope);
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
