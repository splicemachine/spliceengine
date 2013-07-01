package com.splicemachine.si.impl;

import java.util.Set;

public class ConflictResults {
    final Set<Long> toRollForward;
    final Set<Long> childConflicts;
    final Boolean hasTombstone;

    public ConflictResults(Set<Long> toRollForward, Set<Long> childConflicts, Boolean hasTombstone) {
        this.toRollForward = toRollForward;
        this.childConflicts = childConflicts;
        this.hasTombstone = hasTombstone;
    }
}
