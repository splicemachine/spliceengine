package com.splicemachine.si.impl;

import com.splicemachine.si.api.TransactionId;

class TransactionParams {
    final TransactionId parent;
    final Boolean dependent;
    final boolean allowWrites;
    final Boolean readUncommitted;
    final Boolean readCommitted;

    TransactionParams(TransactionId parent, Boolean dependent, boolean allowWrites, Boolean readUncommitted, Boolean readCommitted) {
        this.parent = parent;
        this.dependent = dependent;
        this.allowWrites = allowWrites;
        this.readUncommitted = readUncommitted;
        this.readCommitted = readCommitted;
    }
}
