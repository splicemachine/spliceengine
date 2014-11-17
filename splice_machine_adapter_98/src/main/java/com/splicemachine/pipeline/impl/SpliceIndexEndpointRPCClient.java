package com.splicemachine.pipeline.impl;

import com.google.protobuf.RpcController;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.hbase.table.SpliceRpcController;
import com.splicemachine.pipeline.utils.PipelineUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;

import java.io.IOException;

/**
 * Call SpliceIndexEndpoint via coprocessor RPC framework.
 *
 * TODO: (1) improved exception handling (2) avoid retry?
 */
class SpliceIndexEndpointRPCClient {

    private final HConnection connection;
    private final byte[] tableName;

    public SpliceIndexEndpointRPCClient(HConnection connection, byte[] tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    BulkWritesResult bulkWrite(final BulkWrites writes) throws IOException {
        byte[] startKey = writes.getRegionKey();
        final BlockingRpcCallback<SpliceMessage.BulkWriteResponse> callback = new BlockingRpcCallback<SpliceMessage.BulkWriteResponse>();

        try {
            HTableInterface table = connection.getTable(tableName);
            table.coprocessorService(SpliceMessage.SpliceIndexService.class, startKey, HConstants.EMPTY_END_ROW,
                    new Batch.Call<SpliceMessage.SpliceIndexService, Object>() {
                        @Override
                        public Object call(SpliceMessage.SpliceIndexService instance) throws IOException {
                            RpcController controller = new SpliceRpcController();
                            SpliceMessage.BulkWriteRequest.Builder builder = SpliceMessage.BulkWriteRequest.newBuilder();
                            byte[] requestBytes = PipelineUtils.toCompressedBytes(writes);
                            builder.setBytes(ZeroCopyLiteralByteString.copyFrom(requestBytes));
                            SpliceMessage.BulkWriteRequest request = builder.build();
                            instance.bulkWrite(controller, request, callback);
                            return null;
                        }
                    });
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }

        SpliceMessage.BulkWriteResponse bulkWriteResponse = callback.get();
        byte[] responseBytes = bulkWriteResponse.getBytes().toByteArray();
        return PipelineUtils.fromCompressedBytes(responseBytes, BulkWritesResult.class);
    }


}
