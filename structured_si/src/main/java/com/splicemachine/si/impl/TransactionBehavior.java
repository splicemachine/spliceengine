package com.splicemachine.si.impl;

public interface TransactionBehavior {
    Long getGlobalCommitTimestamp(ImmutableTransaction scope, Long globalCommitTimestamp, Long commitTimestamp,
                                  Transaction parent);
    boolean collapsible();
    boolean needsGlobalCommitTimestamp();
}
