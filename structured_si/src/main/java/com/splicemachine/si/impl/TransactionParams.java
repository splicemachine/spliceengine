package com.splicemachine.si.impl;

/**
 * Package up several transaction parameters so they can be passed around as a single object.
 */
class TransactionParams {
    final TransactionId parent;
    final boolean dependent;
    final boolean allowWrites;
    final Boolean readUncommitted;
    final Boolean readCommitted;

    TransactionParams(TransactionId parent, boolean dependent, boolean allowWrites, Boolean readUncommitted, Boolean readCommitted) {
        this.parent = parent;
        this.dependent = dependent;
        this.allowWrites = allowWrites;
        this.readUncommitted = readUncommitted;
        this.readCommitted = readCommitted;
    }
}
