package com.splicemachine.timestamp.hbase;

import com.splicemachine.primitives.Bytes;
import com.splicemachine.timestamp.impl.TimestampIOException;
import com.splicemachine.timestamp.api.TimestampDataSource;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * Created by jleach on 12/9/15.
 */
public class HBaseTimestampDataSource implements TimestampDataSource {
    private static final Logger LOG = Logger.getLogger(HBaseTimestampDataSource.class);

    private final RecoverableZooKeeper rzk;
    private final String blockNode;
    private final int blockSize;

    /**
     *
     * @param rzk
     * @param blockNode Pointer to the specific znode instance that is specifically configured for timestamp block storage
     */
    public HBaseTimestampDataSource(RecoverableZooKeeper rzk, String blockNode, int blockSize) {
        this.rzk = rzk;
        this.blockNode = blockNode;
        this.blockSize = blockSize;
    }


    public void reserveNextBlock(long nextMax) throws TimestampIOException {
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
            throw new TimestampIOException("count not initialize timestamp data source",e);
        }
    }
}
