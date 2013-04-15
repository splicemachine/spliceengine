package com.splicemachine.si2.impl;

public class ImmutableTransactionStruct {
    public final long beginTimestamp;
    public final ImmutableTransactionStruct immutableParent;
    public final Boolean dependent;
    public final boolean allowWrites;
    public final Boolean readUncommitted;
    public final Boolean readCommitted;

    public ImmutableTransactionStruct(Boolean dependent, boolean allowWrites, Boolean readCommitted,
                                      ImmutableTransactionStruct immutableParent, Boolean readUncommitted, long beginTimestamp) {
        this.dependent = dependent;
        this.allowWrites = allowWrites;
        this.readCommitted = readCommitted;
        this.immutableParent = immutableParent;
        this.readUncommitted = readUncommitted;
        this.beginTimestamp = beginTimestamp;
    }

    public boolean getEffectiveReadUncommitted() {
        if (readUncommitted == null) {
            return immutableParent.getEffectiveReadUncommitted();
        }
        return readUncommitted;
    }

    public boolean getEffectiveReadCommitted() {
        if (readCommitted == null) {
            return immutableParent.getEffectiveReadCommitted();
        }
        return readCommitted;
    }

    public long getRootBeginTimestamp() {
        if (immutableParent == null) {
            return beginTimestamp;
        }
        return immutableParent.getRootBeginTimestamp();
    }
}
