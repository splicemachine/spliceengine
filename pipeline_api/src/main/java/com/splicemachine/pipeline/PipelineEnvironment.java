package com.splicemachine.pipeline;

import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.PipelineMeter;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.si.impl.driver.SIEnvironment;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public interface PipelineEnvironment extends SIEnvironment{
    PipelineExceptionFactory pipelineExceptionFactory();

    PipelineDriver getPipelineDriver();

    ContextFactoryDriver contextFactoryDriver();

    PipelineCompressor pipelineCompressor();

    BulkWriterFactory writerFactory();

    PipelineMeter pipelineMeter();
}
