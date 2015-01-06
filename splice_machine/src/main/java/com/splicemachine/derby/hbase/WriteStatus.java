package com.splicemachine.derby.hbase;

public class WriteStatus {
    int dependentWriteThreads;
    int independentWriteThreads;
    int dependentWriteCount;
    int independentWriteCount;

    public WriteStatus(int dependentWriteThreads, int dependentWriteCount,
                       int independentWriteCount, int independentWriteThreads) {
        assert (dependentWriteThreads >= 0 &&
                independentWriteThreads >= 0 &&
                dependentWriteCount >= 0 &&
                independentWriteCount >= 0);
        this.dependentWriteThreads = dependentWriteThreads;
        this.independentWriteThreads = independentWriteThreads;
        this.dependentWriteCount = dependentWriteCount;
        this.independentWriteCount = independentWriteCount;
    }


    public static WriteStatus incrementDependentWriteStatus(WriteStatus clone, int writes) {
        return new WriteStatus(clone.dependentWriteThreads + 1, clone.dependentWriteCount + writes,
                clone.independentWriteCount, clone.independentWriteThreads);
    }

    public static WriteStatus incrementIndependentWriteStatus(WriteStatus clone, int writes) {
        return new WriteStatus(clone.dependentWriteThreads, clone.dependentWriteCount,
                clone.independentWriteCount + writes, clone.independentWriteThreads + 1);
    }

    public static WriteStatus decrementDependentWriteStatus(WriteStatus clone, int writes) {
        return new WriteStatus(clone.dependentWriteThreads - 1, clone.dependentWriteCount - writes,
                clone.independentWriteCount, clone.independentWriteThreads);
    }

    public static WriteStatus decrementIndependentWriteStatus(WriteStatus clone, int writes) {
        return new WriteStatus(clone.dependentWriteThreads, clone.dependentWriteCount,
                clone.independentWriteCount - writes, clone.independentWriteThreads - 1);
    }

    @Override
    public String toString() {
        return String.format("{ dependentWriteThreads=%d, independentWriteThreads=%d, "
                        + "dependentWriteCount=%d, independentWriteCount=%d }", dependentWriteThreads, independentWriteThreads,
                dependentWriteCount, independentWriteCount);
    }

    public int getDependentWriteThreads() {
        return dependentWriteThreads;
    }

    public int getIndependentWriteThreads() {
        return independentWriteThreads;
    }

    public int getDependentWriteCount() {
        return dependentWriteCount;
    }

    public int getIndependentWriteCount() {
        return independentWriteCount;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || (obj instanceof WriteStatus) &&
                this.dependentWriteCount == ((WriteStatus) obj).dependentWriteCount &&
                this.dependentWriteThreads == ((WriteStatus) obj).dependentWriteThreads &&
                this.independentWriteCount == ((WriteStatus) obj).independentWriteCount &&
                this.independentWriteThreads == ((WriteStatus) obj).independentWriteThreads;
    }

}