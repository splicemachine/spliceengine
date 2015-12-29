package com.splicemachine.derby.hbase;

import com.splicemachine.pipeline.PipelineWriter;
import com.splicemachine.pipeline.api.BulkWriter;
import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.WritePipelineFactory;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.pipeline.client.BulkWritesRPCInvoker;
import com.splicemachine.pipeline.client.RpcChannelFactory;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class CoprocessorWriterFactory implements BulkWriterFactory{
    private final PipelineExceptionFactory exceptionFactory;
    private final PipelineWriter pipelineWriter;
    private final WritePipelineFactory pipelineWriteFactory;
    private final PipelineCompressor compressor;
    private final PartitionInfoCache partitionInfoCache;
    private final RpcChannelFactory channelFactory;

    public CoprocessorWriterFactory(PipelineWriter pipelineWriter,
                                    WritePipelineFactory pipelineWriteFactory,
                                    PipelineCompressor compressor,
                                    PartitionInfoCache partitionInfoCache,
                                    PipelineExceptionFactory exceptionFactory,
                                    RpcChannelFactory channelFactory){
        this.exceptionFactory=exceptionFactory;
        this.pipelineWriter=pipelineWriter;
        this.pipelineWriteFactory = pipelineWriteFactory;
        this.compressor = compressor;
        this.partitionInfoCache = partitionInfoCache;
        this.channelFactory=channelFactory;
    }

    @Override
    public BulkWriter newWriter(byte[] tableName){
        return new BulkWritesRPCInvoker(tableName,pipelineWriter,
                pipelineWriteFactory,compressor,
                exceptionFactory,channelFactory,partitionInfoCache);
    }

    @Override
    public void invalidateCache(byte[] tableName) throws IOException{
        partitionInfoCache.invalidate(tableName);
    }
}
