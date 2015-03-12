package com.splicemachine.stats;

import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.stats.estimate.Distribution;

import java.util.List;

/**
 * Representation of Partition-level statistics.
 *
 * Partition-level statistics are statistics aboud the partition itself--how many rows,
 * the width of those rows, and some physical statistics about that. These
 * partitions are designed to be mergeable into either a server-level, or a table-level
 * view of that data.
 *
 * @author Scott Fines
 *         Date: 2/23/15
 */
@ThreadSafe
public interface PartitionStatistics extends Mergeable<PartitionStatistics> {
    /**
     * @return the total number of rows in the partition.
     */
    long rowCount();

    /**
     * @return the total size of the partition (in bytes).
     */
    long totalSize();

    /**
     * @return the average width of a single row (in bytes) in this partition. This includes
     * the row key and cell contents.
     */
    int avgRowWidth();

    /**
     * @return the 5-minute exponentially weighted moving average number of queries executed
     * against this partition
     */
    long queryCount();

    /**
     * @return a unique identifier for this partition
     */
    String partitionId();

    /**
     * @return the unique identifier for the table to which this partition belongs
     */
    String tableId();

    /**
     * @return the (estimated) latency to read a single row from local storage(measured in microseconds)
     */
    double localReadLatency();

    /**
     * @return the (estimated) latency to read a single row from this partition locally(measured
     *          in microseconds)
     */
    double remoteReadLatency();

    /**
     * @return the time required to read the entire partition (measured in microseconds). This can be
     * measured or estimated.
     */
    long localReadTime();

    /**
     * @return the time required to read the entire partition (measured in microseconds). This can be
     * measured or estimated.
     */
    long remoteReadTime();

    /**
     * @return the time taken to collect statistics against this partition
     *          (measured in microseconds)
     */
    long collectionTime();

    /**
     * @return Statistics about individual columns (which were most recently collected).
     */
    List<ColumnStatistics> columnStatistics();

    /**
     * @param columnId the identifier of the column to fetch(indexed from 0)
     * @param <T> the expected type of statistics to return
     * @return statistics for the column, if such statistics exist, or {@code null} if
     * no statistics are available for that column.
     */
    <T> ColumnStatistics<T> columnStatistics(int columnId);

    <T> Distribution<T> columnDistribution(int columnId);
}
