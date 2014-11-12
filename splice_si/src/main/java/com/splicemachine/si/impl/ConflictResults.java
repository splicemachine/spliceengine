package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongOpenHashSet;

import java.util.Collections;
import java.util.Set;

public class ConflictResults {
    public static final ConflictResults NO_CONFLICT=new ConflictResults(LongOpenHashSet.newInstanceWithCapacity(0,0.9f), LongOpenHashSet.from(), false);
    private LongOpenHashSet toRollForward;
    LongOpenHashSet childConflicts;
    private LongOpenHashSet additiveConflicts;
    boolean hasTombstone;

    public ConflictResults(){

    }

    public ConflictResults(LongOpenHashSet toRollForward, LongOpenHashSet childConflicts, Boolean hasTombstone) {
        this.toRollForward = toRollForward;
        this.childConflicts = childConflicts;
        this.hasTombstone = hasTombstone;
    }


    public void child(long txnId){
        if(childConflicts==null)
            childConflicts = LongOpenHashSet.newInstanceWithCapacity(1,0.9f);
        childConflicts.add(txnId);
    }

    public void additive(long txnId){
        if(additiveConflicts==null)
            additiveConflicts = LongOpenHashSet.newInstanceWithCapacity(1,0.9f);
        additiveConflicts.add(txnId);
    }

    public void addRollForward(long dataTransactionId) {
        if(toRollForward==null)
            toRollForward = LongOpenHashSet.newInstanceWithCapacity(1,0.9f);
        toRollForward.add(dataTransactionId);
    }

    public void tombstone(boolean hasTombstone) {
        this.hasTombstone = hasTombstone;
    }

    public boolean hasAdditiveConflicts() {
        return additiveConflicts!=null;
    }
}
