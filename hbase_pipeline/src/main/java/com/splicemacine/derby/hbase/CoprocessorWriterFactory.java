package com.splicemacine.derby.hbase;

import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.pipeline.PipelineWriter;
import com.splicemachine.pipeline.api.BulkWriter;
import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.WritePipelineFactory;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemacine.pipeline.client.BulkWritesRPCInvoker;
import org.apache.hadoop.hbase.client.HConnection;

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

    public CoprocessorWriterFactory(PipelineWriter pipelineWriter,
                                    WritePipelineFactory pipelineWriteFactory,
                                    PipelineCompressor compressor,
                                    PipelineExceptionFactory exceptionFactory){
        this.exceptionFactory=exceptionFactory;
        this.pipelineWriter=pipelineWriter;
        this.pipelineWriteFactory = pipelineWriteFactory;
        this.compressor = compressor;
    }

    @Override
    public BulkWriter newWriter(byte[] tableName){
        return new BulkWritesRPCInvoker(tableName,pipelineWriter,pipelineWriteFactory,compressor,exceptionFactory);
    }

    @Override
    public void invalidateCache(byte[] tableName) throws IOException{
        ((HConnection)HBaseConnectionFactory.getInstance().getConnection()).clearRegionCache(tableName);
    }
}
