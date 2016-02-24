package com.splicemachine.pipeline.testsetup;

import com.splicemachine.derby.hbase.HPipelineExceptionFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.si.testsetup.HBaseDataEnv;

/**
 * @author Scott Fines
 *         Date: 2/25/16
 */
public class HPipelineTestDataEnv extends HBaseDataEnv implements PipelineTestDataEnv{
    @Override
    public PipelineExceptionFactory pipelineExceptionFactory(){
        return HPipelineExceptionFactory.INSTANCE;
    }
}
