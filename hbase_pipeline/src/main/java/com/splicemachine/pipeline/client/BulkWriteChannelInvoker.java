package com.splicemachine.pipeline.client;

import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.WrongPartitionException;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.hbase.SpliceRpcController;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.PartitionInfoCache;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import java.io.IOException;
import java.net.ConnectException;

/**
 * @author Scott Fines
 *         Date: 3/20/14
 */
public class BulkWriteChannelInvoker {
    private final byte[] tableName;
    private final PipelineExceptionFactory pef;
    private final PipelineCompressor compressor;
    private final RpcChannelFactory channelFactory;
    private final PartitionInfoCache partitionInfoCache;
    private final HBaseTableInfoFactory tableInfoFactory;

    public BulkWriteChannelInvoker(byte[] tableName,
                                   PipelineCompressor pipelineCompressor,
                                   RpcChannelFactory channelFactory,
                                   PartitionInfoCache partitionInfoCache,
                                   PipelineExceptionFactory pef,
                                   HBaseTableInfoFactory tableInfoFactory) {
        this.tableName = tableName;
        this.pef = pef;
        this.compressor = pipelineCompressor;
        this.channelFactory = channelFactory;
        this.partitionInfoCache =partitionInfoCache;
        this.tableInfoFactory=tableInfoFactory;
    }

    public BulkWritesResult invoke(BulkWrites write) throws IOException {
        TableName tableName=tableInfoFactory.getTableInfo(this.tableName);
        CoprocessorRpcChannel channel = channelFactory.newChannel(tableName,write.getRegionKey());

        boolean cacheCheck = false;
        try {
            SpliceMessage.SpliceIndexService service = ProtobufUtil.newServiceStub(SpliceMessage.SpliceIndexService.class, channel);
            SpliceMessage.BulkWriteRequest.Builder builder = SpliceMessage.BulkWriteRequest.newBuilder();
            byte[] requestBytes = compressor.compress(write);
            builder.setBytes(ZeroCopyLiteralByteString.copyFrom(requestBytes));
            SpliceMessage.BulkWriteRequest bwr = builder.build();

            BlockingRpcCallback<SpliceMessage.BulkWriteResponse> doneCallback =new BlockingRpcCallback<>();
            SpliceRpcController controller = new SpliceRpcController();

            service.bulkWrite(controller, bwr, doneCallback);
            Throwable error = controller.getThrowable();
            if (error != null) {
            	clearCacheIfNeeded(error);
            	cacheCheck = true;
                throw pef.processRemoteException(error);
            }
            SpliceMessage.BulkWriteResponse bulkWriteResponse = doneCallback.get();
            byte[] bytes = bulkWriteResponse.getBytes().toByteArray();

            return compressor.decompress(bytes,BulkWritesResult.class);
        } catch (Exception e) {
        	if (!cacheCheck) clearCacheIfNeeded(e);
            throw pef.processRemoteException(e);
        }
    }

    private boolean clearCacheIfNeeded(Throwable e) throws IOException{
        if (e instanceof WrongPartitionException ||
            e instanceof NotServingRegionException ||
            e instanceof ConnectException ||
            isFailedServerException(e)) {
            /*
             * We sent it to the wrong place, so we need to resubmit it. But since we
             * pulled it from the cache, we first invalidate that cache
             */
            TableName tableNameObj=tableInfoFactory.getTableInfo(this.tableName);
            partitionInfoCache.invalidate(tableNameObj);
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
