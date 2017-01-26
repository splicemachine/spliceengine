/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
