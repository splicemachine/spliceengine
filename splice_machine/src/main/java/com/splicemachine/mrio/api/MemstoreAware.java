package com.splicemachine.mrio.api;

/**
 * 
 * Class for capturing the state of the memstore to make sure scans only occur when their underlying file system is stable.
 *
 */
public class MemstoreAware {
    public boolean splitMerge = false;
    public int flushCount;
    public int compactionCount;
    public int scannerCount;

    public MemstoreAware() {
        this.splitMerge = false;
        this.flushCount = 0;
        this.compactionCount = 0;
        this.scannerCount = 0;
    }
    
    public MemstoreAware(boolean splitMerge, int flushCount, int compactionCount, int scannerCount) {
        this.splitMerge = splitMerge;
        this.flushCount = flushCount;
        this.compactionCount = compactionCount;
        this.scannerCount = scannerCount;
    }
    
    

        public static MemstoreAware changeSplitMerge(MemstoreAware clone, boolean splitMerge) {
            return new MemstoreAware(splitMerge, clone.flushCount,
                    clone.compactionCount, clone.scannerCount);
        }

        public static MemstoreAware incrementFlushCount(MemstoreAware clone) {
            return new MemstoreAware(clone.splitMerge, clone.flushCount+1,
                    clone.compactionCount, clone.scannerCount);
        }

        public static MemstoreAware decrementFlushCount(MemstoreAware clone) {
            return new MemstoreAware(clone.splitMerge, clone.flushCount-1,
                    clone.compactionCount, clone.scannerCount);
        }
       
        public static MemstoreAware incrementCompactionCount(MemstoreAware clone) {
            return new MemstoreAware(clone.splitMerge, clone.flushCount,
                    clone.compactionCount+1, clone.scannerCount);
        }

        public static MemstoreAware decrementCompactionCount(MemstoreAware clone) {
            return new MemstoreAware(clone.splitMerge, clone.flushCount,
                    clone.compactionCount+1, clone.scannerCount);
        }

        public static MemstoreAware incrementScannerCount(MemstoreAware clone) {
            return new MemstoreAware(clone.splitMerge, clone.flushCount,
                    clone.compactionCount, clone.scannerCount+1);
        }

        public static MemstoreAware decrementScannerCount(MemstoreAware clone) {
            return new MemstoreAware(clone.splitMerge, clone.flushCount,
                    clone.compactionCount, clone.scannerCount-1);
        }

	
}
