package com.splicemachine.si.api;

import org.apache.hadoop.hbase.util.Pair;

import java.util.Set;

public class PutToRun<Mutation, Lock> {
    public final Pair<Mutation, Lock> putAndLock;
    public final Set<Long> conflictingChildren;

    public PutToRun(Pair<Mutation, Lock> putAndLock, Set<Long> conflictingChildren) {
        this.putAndLock = putAndLock;
        this.conflictingChildren = conflictingChildren;
    }
}
