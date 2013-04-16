package com.splicemachine.si.impl;

public class ImmutableTransaction {
    public final long beginTimestamp;
    protected final Boolean dependent;

    private final ImmutableTransaction immutableParent;
    private final boolean allowWrites;
    private final Boolean readUncommitted;
    private final Boolean readCommitted;

    public ImmutableTransaction(Boolean dependent, boolean allowWrites, Boolean readCommitted,
                                ImmutableTransaction immutableParent, Boolean readUncommitted, long beginTimestamp) {
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

    public boolean isReadOnly() {
        return !allowWrites;
    }

}
