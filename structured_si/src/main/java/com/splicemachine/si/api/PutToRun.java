package com.splicemachine.si.api;

import com.splicemachine.si.data.api.SRowLock;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Set;

public class PutToRun<Mutation, Lock extends SRowLock> {
    public final Pair<Mutation, Integer> putAndLock;
    public final Lock lock;
    public final Set<Long> conflictingChildren;

    public PutToRun(Pair<Mutation, Integer> putAndLock, Lock lock, Set<Long> conflictingChildren) {
        this.putAndLock = putAndLock;
        this.lock = lock;
        this.conflictingChildren = conflictingChildren;
    }
}
