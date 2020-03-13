/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.pipeline.client;

import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.access.api.NotServingPartitionException;
import com.splicemachine.access.api.WrongPartitionException;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.ipc.RpcChannelFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.storage.PartitionInfoCache;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.NoServerForRegionException;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils.BlockingRpcCallback;

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
            ServerRpcController controller = new ServerRpcController();
            service.bulkWrite(controller, bwr, doneCallback);
            if (controller.failed()){
                IOException error=controller.getFailedOn();
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
                e instanceof NoServerForRegionException ||
            isFailedServerException(e)) {
            /*
             * We sent it to the wrong place, so we need to resubmit it. But since we
             * pulled it from the cache, we first invalidate that cache
             */
            partitionInfoCache.invalidate(this.tableName);
            partitionInfoCache.invalidateAdapter(this.tableName);
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
