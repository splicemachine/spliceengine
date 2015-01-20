package com.splicemachine.pipeline.impl;

import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.hbase.NoRetryCoprocessorRpcChannel;
import com.splicemachine.hbase.table.IncorrectRegionException;
import com.splicemachine.hbase.table.SpliceRpcController;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.utils.PipelineUtils;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 3/20/14
 */
public class BulkWriteChannelInvoker {

    private final HConnection connection;
    private final byte[] tableName;

    public BulkWriteChannelInvoker(HConnection connection, byte[] tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    public BulkWritesResult invoke(BulkWrites write) throws IOException {
        NoRetryCoprocessorRpcChannel channel
                = new NoRetryCoprocessorRpcChannel(connection, TableName.valueOf(tableName), write.getRegionKey());

        try {
            SpliceMessage.SpliceIndexService service = ProtobufUtil.newServiceStub(SpliceMessage.SpliceIndexService.class, channel);

            SpliceMessage.BulkWriteRequest.Builder builder = SpliceMessage.BulkWriteRequest.newBuilder();
            byte[] requestBytes = PipelineEncoding.encode(write);
//            byte[] requestBytes = PipelineUtils.toCompressedBytes(write);
            builder.setBytes(ZeroCopyLiteralByteString.copyFrom(requestBytes));
            SpliceMessage.BulkWriteRequest bwr = builder.build();

            BlockingRpcCallback<SpliceMessage.BulkWriteResponse> doneCallback = new BlockingRpcCallback<SpliceMessage.BulkWriteResponse>();
            SpliceRpcController controller = new SpliceRpcController();

            service.bulkWrite(controller, bwr, doneCallback);
            Throwable error = controller.getThrowable();

            if (error != null) {
                if (error instanceof IncorrectRegionException || error instanceof NotServingRegionException) {
                    /*
                     * We sent it to the wrong place, so we need to resubmit it. But since we
                     * pulled it from the cache, we first invalidate that cache
                     */
                    connection.clearRegionCache(TableName.valueOf(tableName));
                }
                throw Exceptions.getIOException(error);
            }
            SpliceMessage.BulkWriteResponse bulkWriteResponse = doneCallback.get();
            byte[] bytes = bulkWriteResponse.getBytes().toByteArray();

            return PipelineUtils.fromCompressedBytes(bytes, BulkWritesResult.class);
        } catch (Exception e) {
            throw Exceptions.getIOException(e);
        }
    }

}
