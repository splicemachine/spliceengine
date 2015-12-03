package com.splicemachine.pipeline.impl;

import com.google.protobuf.SpliceZeroCopyByteString;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.hbase.NoRetryCoprocessorRpcChannel;
import com.splicemachine.hbase.table.IncorrectRegionException;
import com.splicemachine.hbase.table.SpliceRpcController;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.ConnectException;

/**
 * @author Scott Fines
 *         Date: 3/20/14
 */
public class BulkWriteChannelInvoker {
	private static Logger LOG = Logger.getLogger(BulkWriteChannelInvoker.class);
    private final HConnection connection;
    private final byte[] tableName;

    public BulkWriteChannelInvoker(HConnection connection, byte[] tableName) {
        this.connection = connection;
        this.tableName = tableName;
    }

    public BulkWritesResult invoke(BulkWrites write) throws IOException {
        NoRetryCoprocessorRpcChannel channel
                = new NoRetryCoprocessorRpcChannel(connection, TableName.valueOf(tableName), write.getRegionKey());

        boolean cacheCheck = false;
        try {
            SpliceMessage.SpliceIndexService service = ProtobufUtil.newServiceStub(SpliceMessage.SpliceIndexService.class, channel);

            SpliceMessage.BulkWriteRequest.Builder builder = SpliceMessage.BulkWriteRequest.newBuilder();
            byte[] requestBytes = PipelineEncoding.encode(write);
//            byte[] requestBytes = PipelineUtils.toCompressedBytes(write);
            builder.setBytes(SpliceZeroCopyByteString.copyFrom(requestBytes));
            SpliceMessage.BulkWriteRequest bwr = builder.build();

            BlockingRpcCallback<SpliceMessage.BulkWriteResponse> doneCallback = new BlockingRpcCallback<SpliceMessage.BulkWriteResponse>();
            SpliceRpcController controller = new SpliceRpcController();

            service.bulkWrite(controller, bwr, doneCallback);
            Throwable error = controller.getThrowable();
            if (error != null) {
            	clearCacheIfNeeded(error);
            	cacheCheck = true;
                throw Exceptions.getIOException(error);
            }
            SpliceMessage.BulkWriteResponse bulkWriteResponse = doneCallback.get();
            byte[] bytes = bulkWriteResponse.getBytes().toByteArray();

            return PipelineUtils.fromCompressedBytes(bytes, BulkWritesResult.class);
        } catch (Exception e) {
        	if (!cacheCheck) clearCacheIfNeeded(e);
            throw Exceptions.getIOException(e);
        }
    }

    private boolean clearCacheIfNeeded(Throwable e) {
        if (e instanceof IncorrectRegionException ||
            e instanceof NotServingRegionException ||
            e instanceof ConnectException ||
            isFailedServerException(e)) {
            /*
             * We sent it to the wrong place, so we need to resubmit it. But since we
             * pulled it from the cache, we first invalidate that cache
             */
        	if (LOG.isTraceEnabled())
        		SpliceLogUtils.trace(LOG, "Clearing stale region cache for table %s", Bytes.toString(tableName));
            connection.clearRegionCache(TableName.valueOf(tableName));
            return true;
	    }
        return false;
    }
    
    private static boolean isFailedServerException(Throwable t) {
    	// Unfortunately we can not call ExceptionTranslator.isFailedServerException()
    	// which is explicitly for this purpose. Other places in the code call it,
    	// but SpliceHTabe is already in splice_si_adapter_98 so we can not use
    	// the generic DerbyFactory capability without a bunch of refactoring.
    	// We'll come back to this later.
    	return t.getClass().getName().contains("FailedServerException");
    }

}
