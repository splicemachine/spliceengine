package com.splicemachine.pipeline.testsetup;


import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.mem.DirectPipelineExceptionFactory;
import com.splicemachine.si.MemSIDataEnv;

/**
 * @author Scott Fines
 *         Date: 2/8/16
 */
public class MPipelineTestDataEnv extends MemSIDataEnv implements PipelineTestDataEnv{
    @Override
    public PipelineExceptionFactory pipelineExceptionFactory(){
        return DirectPipelineExceptionFactory.INSTANCE;
    }
}
