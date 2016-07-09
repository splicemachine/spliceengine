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

    Set<PartitionLoad> getPartitionLoads();
}
