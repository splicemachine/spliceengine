package com.splicemachine.storage;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public interface PartitionServerLoad{

    int numPartitions();

    long totalWriteRequests();

    long totalReadRequests();

    long totalRequests();

    /**
     * Optional method.
     *
     * @return the size of the compaction queue,
     * or {@code -1} if compactions are not performed in
     * this architecture
     */
    int compactionQueueLength();

    /**
     * Optional method.
     *
     * @return the size of the flush queue,
     * or {@code -1} if flushes are not performed in
     * this architecture
     */
    int flushQueueLength();

    Set<PartitionLoad> getPartitionLoads();
}
