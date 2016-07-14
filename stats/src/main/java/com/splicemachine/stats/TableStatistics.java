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
 * Representation of Table-level statistics.
 *
 * Table statistics are statistics about the overall table itself, accumulated over all partitions.
 *
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface TableStatistics extends PartitionStatistics{

    /*
     * Note: A lot of these methods override methods in PartitionStatistics. This
     * Override behavior is so that more precise documentation can be provided. Please
     * remember this when adding methods to PartitionStatistics
     */

    /**
     * @return the total number of entries in the table across all partitions
     */
    @Override
    long rowCount();

    /**
     * @return the total size of the table across all partitions
     */
    @Override
    long totalSize();

    /**
     * @return the average size of a single partition across all partitions.
     */
    long avgSize();

    /**
     * @return the <em>average</em> "local" read latency of a single entry. This is averaged
     * across all partitions.
     */
    @Override
    double localReadLatency();

    /**
     * @return the "local" read latency of a single entry. Note that this is the <em>average</em>
     * across all partitions
     */
    @Override
    double remoteReadLatency();

    /**
     * @return the average width of a single row (in bytes) across all partitions. This includes
     * the row key and cell contents.
     */
    @Override
    int avgRowWidth();

    /**
     * @return the local read time, averaged over all partitions
     */
    @Override
    long localReadTime();

    /**
     * @return the remote read time, averaged over all partitions
     */
    @Override
    long remoteReadTime();

    /**
     * @return the 5-minute exponentially weighted moving average number of queries executed
     * against this table across all partitions.
     */
    @Override
    long queryCount();

    /**
     * Compute the time taken to collect statistics for <em>all</em> partitions.
     *
     * <p>
     *     Note that there are two different ways to compute this number: If the implementation
     *     of statistics collection is <em>sequential</em>(i.e. only on one thread of execution), then
     *     this total should be the sum of the collection times for all partitions. If, however, the
     *     collection is <em>parallel</em>, then this should be the time from start to finish of some
     *     form of collection schema.
     * </p>
     *
     * <p>
     *    Of course, some collection schemes are asynchronous such that not all partitions are collected
     *    in a single command, so measuring total time that way is not acceptable in that case. In those situations,
     *    one should approximate the request by taking the maximum across all partitions.
     * </p>
     *
     * @return the time taken to collect statistics against <em>all</em> partitions.
     */
    @Override
    long collectionTime();

    /**
     * @return Detailed statistics for each partition.
     */
    List<? extends PartitionStatistics> partitionStatistics();
}
