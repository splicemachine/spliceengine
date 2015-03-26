package com.splicemachine.derby.stats;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * Aggregate immutable statistics for a Shuffle/Sink operation.
 *
 * @author Scott Fines
 * Created on: 2/26/13
 */
public class TaskStats implements Externalizable {
    private static final long serialVersionUID = 1l;

    private long totalTime;
    private long totalRowsProcessed;
    private long totalRowsWritten;

    /*
     * An array of booleans indicating which temp buckets were
     * written to by this task. This way, we can set up clients
     * to only open up scanners against regions where data
     * is known to exist (helps for small scans).
     */
    private boolean[] tempBuckets;

    public TaskStats() {
    }

    public TaskStats(long totalTime, long totalRowsProcessed, long totalRowsWritten) {
        this(totalTime, totalRowsProcessed, totalRowsWritten, null);
    }

    public TaskStats(long totalTime, long totalRowsProcessed, long totalRowsWritten, boolean[] tempBuckets) {
        this.totalRowsProcessed = totalRowsProcessed;
        this.totalRowsWritten = totalRowsWritten;
        this.totalTime = totalTime;
        this.tempBuckets = tempBuckets;
    }

    public long getTotalTime() {
        return totalTime;
    }

    public long getTotalRowsProcessed() {
        return totalRowsProcessed;
    }

    public long getTotalRowsWritten() {
        return totalRowsWritten;
    }

    public boolean[] getTempBuckets() {
        return tempBuckets;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(totalRowsProcessed);
        out.writeLong(totalRowsWritten);
        out.writeLong(totalTime);

        out.writeBoolean(tempBuckets != null);
        if (tempBuckets != null) {
            out.writeInt(tempBuckets.length);
            for (boolean tempBucket : tempBuckets)
                out.writeBoolean(tempBucket);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        totalRowsProcessed = in.readLong();
        totalRowsWritten = in.readLong();
        totalTime = in.readLong();

        if (in.readBoolean()) {
            tempBuckets = new boolean[in.readInt()];
            for (int i = 0; i < tempBuckets.length; i++) {
                tempBuckets[i] = in.readBoolean();
            }
        }
    }

    /**
     * Merge/aggregate the stats from multiple instances of this class.
     */
    public static TaskStats merge(List<TaskStats> taskStatsList) {
        TaskStats total = new TaskStats();
        for (TaskStats stat : taskStatsList) {
            total.totalTime += stat.getTotalTime();
            total.totalRowsProcessed += stat.getTotalRowsProcessed();
            total.totalRowsWritten += stat.getTotalRowsWritten();
            if (stat.tempBuckets != null) {
                if (total.tempBuckets == null) {
                    total.tempBuckets = new boolean[stat.tempBuckets.length];
                }
                for (int i = 0; i < stat.tempBuckets.length; i++) {
                    total.tempBuckets[i] = total.tempBuckets[i] || stat.tempBuckets[i];
                }
            }
        }
        return total;
    }

    /**
     * Sum the TotalRowsWritten property of the specified collection of TaskStats.
     */
    public static long sumTotalRowsWritten(Iterable<TaskStats> taskStatsList) {
        long sum = 0;
        for (TaskStats stat : taskStatsList) {
            sum += stat.getTotalRowsWritten();
        }
        return sum;
    }
}

