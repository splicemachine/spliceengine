package com.splicemachine.si.api;

import org.apache.hadoop.hbase.util.Pair;

import java.util.Set;

public class PutToRun<Mutation> {
    public final Pair<Mutation, Integer> putAndLock;
    public final Set<Long> conflictingChildren;

    public PutToRun(Pair<Mutation, Integer> putAndLock, Set<Long> conflictingChildren) {
        this.putAndLock = putAndLock;
        this.conflictingChildren = conflictingChildren;
    }
}
