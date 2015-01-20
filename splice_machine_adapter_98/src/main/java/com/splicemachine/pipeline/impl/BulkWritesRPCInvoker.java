package com.splicemachine.pipeline.impl;

import com.splicemachine.derby.hbase.SpliceBaseIndexEndpoint;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.pipeline.api.BulkWritesInvoker;
import org.apache.hadoop.hbase.client.HConnection;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 1/31/14
 */
public class BulkWritesRPCInvoker implements BulkWritesInvoker {
    public static volatile boolean forceRemote = false;

    private BulkWriteChannelInvoker bulkWriteChannelInvoker;

    public BulkWritesRPCInvoker(HConnection connection, byte[] tableName) {
        this.bulkWriteChannelInvoker = new BulkWriteChannelInvoker(connection, tableName);
    }

    @Override
    public BulkWritesResult invoke(final BulkWrites writes, boolean refreshCache) throws IOException {
        assert writes.numEntries() != 0;
        if(!forceRemote) {
            SpliceDriver spliceDriver = SpliceDriver.driver();

            if (spliceDriver.isStarted()) {
                Iterator<BulkWrite> iterator = writes.getBulkWrites().iterator();
                assert iterator.hasNext(): "invoked a write with no BulkWrite entities!";

                BulkWrite firstBulkWrite = iterator.next();
                String encodedRegionName = firstBulkWrite.getEncodedStringName();
                SpliceBaseIndexEndpoint indexEndpoint = spliceDriver.getIndexEndpoint(encodedRegionName);

                if (indexEndpoint != null) {
                    return indexEndpoint.bulkWrite(writes);
                }
            }
        }
        return bulkWriteChannelInvoker.invoke(writes);
    }


    public static final class Factory implements BulkWritesInvoker.Factory {
        private final HConnection connection;
        private final byte[] tableName;

        public Factory(HConnection connection, byte[] tableName) {
            this.connection = connection;
            this.tableName = tableName;
        }

        @Override
        public BulkWritesInvoker newInstance() {
            return new BulkWritesRPCInvoker(connection, tableName);
        }
    }
}