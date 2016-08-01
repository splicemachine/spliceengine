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

package com.splicemachine.storage.util;

import com.splicemachine.storage.Partition;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.List;

/**
 * Created by jleach on 8/1/16.
 */
public class PartitionUtils {
    protected static final Logger LOG = Logger.getLogger(PartitionUtils.class);

    /**
     * Get Partitions in Range without refreshing the underlying cache.
     *
     * @param partition
     * @param scan
     * @return
     */
    public static List<Partition> getPartitionsInRange(Partition partition, Scan scan) throws IOException {
        return getPartitionsInRange(partition, scan, false);
    }

    /**
     * Get the partitions in range with optional refreshing of the cache
     *
     * @param partition
     * @param scan
     * @param refresh
     * @return
     */
    public static List<Partition> getPartitionsInRange(Partition partition, Scan scan, boolean refresh) throws IOException {
        List<Partition> partitions;
        int i = 0;
        while (true) {
            partitions = partition.subPartitions(scan.getStartRow(), scan.getStopRow(), refresh);
            if (partitions == null || partitions.isEmpty()) {
                if (!refresh) {
                    // try again with a refresh
                    refresh = true;
                    continue;
                } else {
                    i++;
                    // Not Good, partition missing, race condition, allow 100 misses and then bail out...
                    if (i>100) {
                        String msg = String.format("Couldn't find subpartitions in range for %s and scan %s", partition, scan);
                        SpliceLogUtils.error(LOG, msg);
                        throw new IllegalStateException(msg);
                    }
                }
            } else {
                break;
            }
        }
        return partitions;
    }


}
