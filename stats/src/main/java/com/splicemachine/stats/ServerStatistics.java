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
