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
    private PipelineWriter pipelineWriter;
    private WritePipelineFactory pipelineWriteFactory;
    private final PipelineCompressor compressor;
    private final PartitionInfoCache partitionInfoCache;
    private final RpcChannelFactory channelFactory;

    public CoprocessorWriterFactory(PipelineCompressor compressor,
                                    PartitionInfoCache partitionInfoCache,
                                    PipelineExceptionFactory exceptionFactory,
                                    RpcChannelFactory channelFactory){
        this.exceptionFactory=exceptionFactory;
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

    @Override
    public void setPipeline(WritePipelineFactory writePipelineFactory){
       this.pipelineWriteFactory = writePipelineFactory;
    }

    @Override
    public void setWriter(PipelineWriter pipelineWriter){
        this.pipelineWriter = pipelineWriter;
    }
}
