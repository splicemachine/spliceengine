package com.splicemachine.pipeline.mem;

import com.splicemachine.pipeline.PipelineWriter;
import com.splicemachine.pipeline.api.BulkWriter;
import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.WritePipelineFactory;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.traffic.SpliceWriteControl;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
@ThreadSafe
public class DirectBulkWriterFactory implements BulkWriterFactory{
    private final PipelineWriter pipelineWriter;

    public DirectBulkWriterFactory(WritePipelineFactory wpf,
                                   SpliceWriteControl writeControl,
                                   PipelineExceptionFactory exceptionFactory) throws IOException{
        this.pipelineWriter = new PipelineWriter(exceptionFactory,wpf,writeControl);
    }

    public void setWriteCoordinator(WriteCoordinator writeCoordinator){
        this.pipelineWriter.setWriteCoordinator(writeCoordinator);
    }
    @Override
    public BulkWriter newWriter(byte[] tableName){
        return new DirectBulkWriter(pipelineWriter);
    }

    @Override
    public void invalidateCache(byte[] tableName){
        //no-op for in-memory
    }
}
