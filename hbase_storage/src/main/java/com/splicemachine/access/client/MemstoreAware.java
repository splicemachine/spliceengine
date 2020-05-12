/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.access.client;

/**
 * 
 * Class for capturing the state of the memstore to make sure scans only occur when their underlying file system is stable.
 *
 */
public class MemstoreAware {
    public final boolean splitMerge;
    public final int totalFlushCount;
    public final int currentCompactionCount;
    public final int currentScannerCount;
    public final boolean flush;

    public MemstoreAware() {
        this.splitMerge = false;
        this.totalFlushCount = 0;
        this.currentCompactionCount = 0;
        this.currentScannerCount = 0;
        this.flush = false;
    }
    
    public MemstoreAware(boolean splitMerge, int totalFlushCount, int currentCompactionCount, int currentScannerCount, boolean isFlushing) {
        this.splitMerge = splitMerge;
        this.totalFlushCount = totalFlushCount;
        this.currentCompactionCount = currentCompactionCount;
        this.currentScannerCount = currentScannerCount;
        this.flush = isFlushing;
    }
    
    public static MemstoreAware changeSplitMerge(MemstoreAware clone, boolean splitMerge) {
        return new MemstoreAware(splitMerge, clone.totalFlushCount, clone.currentCompactionCount,
                clone.currentScannerCount,clone.flush);
    }

    public static MemstoreAware changeFlush(MemstoreAware clone, boolean flush) {
        return new MemstoreAware(clone.splitMerge, clone.totalFlushCount+1, clone.currentCompactionCount,
                clone.currentScannerCount,flush);
    }

    public static MemstoreAware incrementCompactionCount(MemstoreAware clone) {
        return new MemstoreAware(clone.splitMerge, clone.totalFlushCount, clone.currentCompactionCount+1,
                clone.currentScannerCount,clone.flush);
    }

    public static MemstoreAware decrementCompactionCount(MemstoreAware clone) {
        return new MemstoreAware(clone.splitMerge, clone.totalFlushCount, clone.currentCompactionCount-1,
                clone.currentScannerCount,clone.flush);
    }

    public static MemstoreAware incrementScannerCount(MemstoreAware clone) {
        return new MemstoreAware(clone.splitMerge, clone.totalFlushCount, clone.currentCompactionCount,
                clone.currentScannerCount+1,clone.flush);
    }

    public static MemstoreAware decrementScannerCount(MemstoreAware clone) {
        return new MemstoreAware(clone.splitMerge, clone.totalFlushCount, clone.currentCompactionCount,
                clone.currentScannerCount-1,clone.flush);
    }

    @Override
    public String toString() {
        return "MemstoreAware{" +
                "splitMerge=" + splitMerge +
                ", totalFlushCount=" + totalFlushCount +
                ", currentCompactionCount=" + currentCompactionCount +
                ", currentScannerCount=" + currentScannerCount +
                ", flush=" + flush +
                '}';
    }
}
