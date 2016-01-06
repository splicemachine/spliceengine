package com.splicemachine.pipeline.testsetup;

import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.si.testenv.SITestDataEnv;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public interface PipelineTestDataEnv extends SITestDataEnv{
    PipelineExceptionFactory pipelineExceptionFactory();
}
