package com.splicemachine.si.impl;

/**
 * Plug point to implement different categories of transactions.
 */
public interface TransactionBehavior {
    Long getEffectiveCommitTimestamp(ImmutableTransaction scope, Long globalCommitTimestamp, Long commitTimestamp,
                                     Transaction parent);
    boolean collapsible();
    boolean needsGlobalCommitTimestamp();
}
