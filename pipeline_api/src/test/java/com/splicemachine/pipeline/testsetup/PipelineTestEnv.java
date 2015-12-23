package com.splicemachine.pipeline.testsetup;

import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;
import com.splicemachine.si.testenv.SITestEnv;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public interface PipelineTestEnv extends SITestEnv{
    WriteCoordinator writeCoordinator();

    ContextFactoryLoader contextFactoryLoader(long conglomerateId);

    PipelineExceptionFactory pipelineExceptionFactory();
}
