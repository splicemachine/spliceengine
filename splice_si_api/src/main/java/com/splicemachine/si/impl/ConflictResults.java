/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongOpenHashSet;

public class ConflictResults {

    /* careful mutable */
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
