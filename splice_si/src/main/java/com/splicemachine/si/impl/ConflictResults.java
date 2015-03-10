package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongOpenHashSet;

public class ConflictResults {

    public static final ConflictResults NO_CONFLICT = new ConflictResults(LongOpenHashSet.from(), false);

    private LongOpenHashSet childConflicts;
    private LongOpenHashSet additiveConflicts;
    private boolean hasTombstone;

    public ConflictResults() {
    }

    private ConflictResults(LongOpenHashSet childConflicts, boolean hasTombstone) {
        this.childConflicts = childConflicts;
        this.hasTombstone = hasTombstone;
    }

    public void addChild(long txnId) {
        if (childConflicts == null) {
            childConflicts = LongOpenHashSet.newInstanceWithCapacity(1, 0.9f);
        }
        childConflicts.add(txnId);
    }

    public void addAdditive(long txnId) {
        if (additiveConflicts == null) {
            additiveConflicts = LongOpenHashSet.newInstanceWithCapacity(1, 0.9f);
        }
        additiveConflicts.add(txnId);
    }

    public boolean hasTombstone() {
        return hasTombstone;
    }

    public void setHasTombstone(boolean hasTombstone) {
        this.hasTombstone = hasTombstone;
    }

    public boolean hasAdditiveConflicts() {
        return additiveConflicts != null;
    }

    public LongOpenHashSet getChildConflicts() {
        return childConflicts;
    }
}
