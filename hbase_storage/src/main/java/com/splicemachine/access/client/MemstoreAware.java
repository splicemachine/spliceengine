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

package com.splicemachine.access.client;

/**
 * 
 * Class for capturing the state of the memstore to make sure scans only occur when their underlying file system is stable.
 *
 */
public class MemstoreAware {
    public boolean splitMerge = false;
    public int totalFlushCount;
    public int currentCompactionCount;
    public int currentScannerCount;
    public int currentFlushCount;

    public MemstoreAware() {
        this.splitMerge = false;
        this.totalFlushCount = 0;
        this.currentCompactionCount = 0;
        this.currentScannerCount = 0;
        this.currentFlushCount = 0;
    }
    
    public MemstoreAware(boolean splitMerge, int totalFlushCount, int currentCompactionCount, int currentScannerCount, int currentFlushCount) {
        this.splitMerge = splitMerge;
        this.totalFlushCount = totalFlushCount;
        this.currentCompactionCount = currentCompactionCount;
        this.currentScannerCount = currentScannerCount;
        this.currentFlushCount = currentFlushCount;
    }
    
    

        public static MemstoreAware changeSplitMerge(MemstoreAware clone, boolean splitMerge) {
            return new MemstoreAware(splitMerge, clone.totalFlushCount, clone.currentCompactionCount,
                    clone.currentScannerCount,clone.currentFlushCount);
        }

        public static MemstoreAware incrementFlushCount(MemstoreAware clone) {
            return new MemstoreAware(clone.splitMerge, clone.totalFlushCount+1, clone.currentCompactionCount,
                    clone.currentScannerCount,clone.currentFlushCount+1);
        }

        public static MemstoreAware decrementFlushCount(MemstoreAware clone) {
            return new MemstoreAware(clone.splitMerge, clone.totalFlushCount, clone.currentCompactionCount,
                    clone.currentScannerCount,clone.currentFlushCount-1);
        }
       
        public static MemstoreAware incrementCompactionCount(MemstoreAware clone) {
            return new MemstoreAware(clone.splitMerge, clone.totalFlushCount, clone.currentCompactionCount+1,
                    clone.currentScannerCount,clone.currentFlushCount);
        }

        public static MemstoreAware decrementCompactionCount(MemstoreAware clone) {
            return new MemstoreAware(clone.splitMerge, clone.totalFlushCount, clone.currentCompactionCount-1,
                    clone.currentScannerCount,clone.currentFlushCount);
        }

        public static MemstoreAware incrementScannerCount(MemstoreAware clone) {
            return new MemstoreAware(clone.splitMerge, clone.totalFlushCount, clone.currentCompactionCount,
                    clone.currentScannerCount+1,clone.currentFlushCount);
        }

        public static MemstoreAware decrementScannerCount(MemstoreAware clone) {
            return new MemstoreAware(clone.splitMerge, clone.totalFlushCount, clone.currentCompactionCount,
                    clone.currentScannerCount-1,clone.currentFlushCount);
        }

    @Override
    public String toString() {
        return "MemstoreAware{" +
                "splitMerge=" + splitMerge +
                ", totalFlushCount=" + totalFlushCount +
                ", currentCompactionCount=" + currentCompactionCount +
                ", currentScannerCount=" + currentScannerCount +
                ", currentFlushCount=" + currentFlushCount +
                '}';
    }
}
