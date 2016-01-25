package com.splicemachine.pipeline.client;

import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.pipeline.PartitionWritePipeline;
import com.splicemachine.pipeline.PipelineWriter;
import com.splicemachine.pipeline.api.BulkWriter;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.WritePipelineFactory;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.storage.PartitionInfoCache;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/31/14
 */
public class BulkWritesRPCInvoker implements BulkWriter{
    public static volatile boolean forceRemote = false;

    private final BulkWriteChannelInvoker bulkWriteChannelInvoker;
    private final WritePipelineFactory pipelineFactory;
    private final PipelineWriter pipelineWriter;

    public BulkWritesRPCInvoker(byte[] tableName,
                                PipelineWriter pipelineWriter,
                                WritePipelineFactory pipelineFactory,
                                PipelineCompressor pipelineCompressor,
                                PipelineExceptionFactory exceptionFactory,
                                RpcChannelFactory channelFactory,
                                PartitionInfoCache partInfoCache,
                                HBaseTableInfoFactory tableInfoFactory) {
        this.pipelineFactory = pipelineFactory;
        this.pipelineWriter = pipelineWriter;
        this.bulkWriteChannelInvoker = new BulkWriteChannelInvoker(tableName,pipelineCompressor,channelFactory,partInfoCache,exceptionFactory,tableInfoFactory);
    }

    @Override
    public BulkWritesResult write(final BulkWrites writes, boolean refreshCache) throws IOException {
        assert writes.numEntries() != 0;
        if(!forceRemote) {

            assert !writes.getBulkWrites().isEmpty(): "Invoked a write with no BulkWrite entities!";

            BulkWrite firstBulkWrite = writes.getBulkWrites().iterator().next();
            String encodedRegionName = firstBulkWrite.getEncodedStringName();
            PartitionWritePipeline pipeline=pipelineFactory.getPipeline(encodedRegionName);
            if(pipeline!=null){
                return pipelineWriter.bulkWrite(writes);
            }
        }

        return bulkWriteChannelInvoker.invoke(writes);
    }
}