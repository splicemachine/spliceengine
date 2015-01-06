package com.splicemachine.pipeline.impl;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceBaseIndexEndpoint;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.NoRetryExecRPCInvoker;
import com.splicemachine.pipeline.api.BulkWritesInvoker;
import com.splicemachine.pipeline.coprocessor.BatchProtocol;
import com.splicemachine.pipeline.utils.PipelineUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;
import java.lang.reflect.Proxy;

/**
 * Invoker for remote procedure call for coprocessor.
 *
 * @author Scott Fines
 *         Date: 1/31/14
 */
public class BulkWritesRPCInvoker implements BulkWritesInvoker {
    private static final Class<BatchProtocol> batchProtocolClass = BatchProtocol.class;
    @SuppressWarnings("unchecked")
    private static final Class<? extends CoprocessorProtocol>[] protoClassArray = new Class[]{batchProtocolClass};
    private final HConnection connection;
    private final byte[] tableName;

    /**
     * Connection based invoker for a table
     */
    public BulkWritesRPCInvoker(HConnection connection, byte[] tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    /**
     * Sends across BulkWrites to a specific region...
     */
    @Override
    public BulkWritesResult invoke(BulkWrites writes, boolean refreshCache) throws IOException {
        assert writes.numEntries() != 0;
        SpliceDriver spliceDriver = SpliceDriver.driver();

        if (spliceDriver.isStarted()) {
            BulkWrite firstBulkWrite = (BulkWrite) writes.getBuffer()[0];
            SpliceBaseIndexEndpoint indexEndpoint = spliceDriver.getIndexEndpoint(firstBulkWrite.getEncodedRegionName());
            if (indexEndpoint != null) {
                return indexEndpoint.bulkWrite(writes);
            }
        }

        // if the region is in another JVM
        Configuration config = SpliceConstants.config;
        NoRetryExecRPCInvoker invoker = new NoRetryExecRPCInvoker(config, connection, batchProtocolClass, tableName, writes.getRegionKey(), refreshCache);
        BatchProtocol instance = (BatchProtocol) Proxy.newProxyInstance(config.getClassLoader(), protoClassArray, invoker);
        byte[] compressedWriteBytes = PipelineUtils.toCompressedBytes(writes);
        byte[] compressedResponseBytes = instance.bulkWrites(compressedWriteBytes);
        return PipelineUtils.fromCompressedBytes(compressedResponseBytes, BulkWritesResult.class);
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
