package com.splicemachine.stats;

import java.util.List;

/**
 * Representation of Table-level statistics.
 *
 * Table statistics are statistics about the overall table itself, accumulated over all partitions.
 *
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface TableStatistics {

    /**
     * @return the total number of rows in the table across all partitions
     */
    long rowCount();

    /**
     * @return the total size of the table (in bytes) across all partitions
     */
    long totalSize();

    /**
     * @return the average width of a single row (in bytes) across all partitions. This includes
     * the row key and cell contents.
     */
    int avgRowWidth();

    /**
     * @return the 5-minute exponentially weighted moving average number of queries executed
     * against this table across all partitions.
     */
    long queryCount();

    /**
     * @return the unique identifier for this table.
     */
    String tableId();

    /**
     * @return a Distribution of partition statistics
     */
    Distribution<PartitionStatistics> partitionStatistics();

    /**
     * @return a Distribution of Server statistics
     */
    Distribution<ServerStatistics> serverStats();

    /**
     * @return Statistics about individual columns.
     */
    List<ColumnStatistics> columnStatistics();

}
