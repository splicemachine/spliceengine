package com.splicemachine.pipeline.api;

import com.splicemachine.pipeline.PipelineWriter;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public interface BulkWriterFactory{

    BulkWriter newWriter(byte[] tableName);

    void invalidateCache(byte[] tableName) throws IOException;

    void setPipeline(WritePipelineFactory writePipelineFactory);

    void setWriter(PipelineWriter pipelineWriter);
}
