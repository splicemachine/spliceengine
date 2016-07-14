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

package com.splicemachine.timestamp.hbase;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.timestamp.api.TimestampBlockManager;
import com.splicemachine.timestamp.api.TimestampIOException;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * TimestampBlockManager which uses ZooKeeper to reserve blocks.
 *
 * Created by jleach on 12/9/15.
 */
public class ZkTimestampBlockManager implements TimestampBlockManager{
    private static final Logger LOG = Logger.getLogger(ZkTimestampBlockManager.class);

    private final RecoverableZooKeeper rzk;
    private final String blockNode;

    /**
     * @param rzk the ZooKeeper node to base off
     * @param blockNode Pointer to the specific znode instance that is specifically configured for timestamp block storage
     */
    public ZkTimestampBlockManager(RecoverableZooKeeper rzk,String blockNode) {
        this.rzk = rzk;
        this.blockNode = blockNode;
    }


    public void reserveNextBlock(long nextMax) throws TimestampIOException{
        byte[] data = Bytes.toBytes(nextMax);
        try {
            rzk.setData(blockNode, data, -1 /* version */); // durably reserve the next block
        } catch (KeeperException | InterruptedException e) {
            throw new TimestampIOException(e);
        }
    }

    @Override
    public long initialize() throws TimestampIOException {
        try {
            byte[] data = rzk.getData(blockNode, false, new Stat());
            long maxReservedTs = Bytes.toLong(data);
            SpliceLogUtils.info(LOG, "Initializing: existing max reserved timestamp = %s", maxReservedTs);
            return maxReservedTs;
        } catch (Exception e) {
            throw new TimestampIOException("could not initialize timestamp data source",e);
        }
    }
}
