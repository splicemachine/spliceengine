package com.splicemachine.si.impl;

public class ImmutableTransactionRelativeState {
    public final long beginTimestamp;
    public final Boolean readUncommitted;
    public final Boolean readCommitted;

    public ImmutableTransactionRelativeState(long beginTimestamp, Boolean readCommitted, Boolean readUncommitted) {
        this.beginTimestamp = beginTimestamp;
        this.readUncommitted = readUncommitted;
        this.readCommitted = readCommitted;
    }
}
