/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
