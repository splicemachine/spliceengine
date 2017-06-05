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

package com.splicemachine.pipeline.client;

import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.access.api.NotServingPartitionException;
import com.splicemachine.access.api.WrongPartitionException;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.hbase.SpliceRpcController;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.storage.PartitionInfoCache;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.log4j.Logger;

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

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
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
            builder.setBytes(ZeroCopyLiteralByteString.wrap(requestBytes));
            SpliceMessage.BulkWriteRequest bwr = builder.build();

            BlockingRpcCallback<SpliceMessage.BulkWriteResponse> doneCallback =new BlockingRpcCallback<>();
            SpliceRpcController controller = new SpliceRpcController();

            service.bulkWrite(controller, bwr, doneCallback);
            if (controller.failed()){
                Throwable error=controller.getThrowable();
                clearCacheIfNeeded(error);
                cacheCheck=true;
                if(error!=null)
                    throw pef.processRemoteException(error);
                else
                    throw pef.fromErrorString(controller.errorText());
            }
            SpliceMessage.BulkWriteResponse bulkWriteResponse = doneCallback.get();
            byte[] bytes = bulkWriteResponse.getBytes().toByteArray();
            if(bytes==null || bytes.length<=0){
                Logger logger=Logger.getLogger(BulkWriteChannelInvoker.class);
                logger.error("zero-length bytes returned with a null error for encodedString: "+write.getBulkWrites().iterator().next().getEncodedStringName());
            }

            return compressor.decompress(bytes,BulkWritesResult.class);
        } catch (Exception e) {
        	if (!cacheCheck) clearCacheIfNeeded(e);
            throw pef.processRemoteException(e);
        }
    }

    private boolean clearCacheIfNeeded(Throwable e) throws IOException{
        if (e==null ||
                e instanceof WrongPartitionException ||
                e instanceof NotServingRegionException ||
                e instanceof NotServingPartitionException ||
                e instanceof ConnectException ||
                e instanceof ConnectionClosingException ||
            isFailedServerException(e)) {
            /*
             * We sent it to the wrong place, so we need to resubmit it. But since we
             * pulled it from the cache, we first invalidate that cache
             */
            partitionInfoCache.invalidate(this.tableName);
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
    	return t!=null && t.getClass().getName().contains("FailedServerException");
    }

}
