package com.splicemachine.stats;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface ServerStatistics extends PartitionStatistics {

    /**
     * @return a distribution of partition statistics for partitions
     *         owned by this particular server
     */
    List<PartitionStatistics> partitionStats();

    /* ******************************************************************************************/
    /*Physical statistics*/
    /**
     * @return the number of cores available for use on this server.
     */
    int availableCores();

    /**
     * @return the maximum amount of memory available
     */
    long memoryCapacity();

    /**
     * @return the maximum number of concurrent network operations available
     */
    int concurrentNetworkOperations();


}
