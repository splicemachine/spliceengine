package com.splicemachine.pipeline.testsetup;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class PipelineTestEnvironment{
    private static volatile PipelineTestEnv testEnv;

    public static PipelineTestEnv loadTestEnvironment(){
        PipelineTestEnv env = testEnv;
        if(env==null){
            env = initializeFullEnvironment();
        }
        return env;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static PipelineTestEnv initializeFullEnvironment(){
        synchronized(PipelineTestEnvironment.class){
            if(testEnv==null){
                ServiceLoader<PipelineTestEnv> load=ServiceLoader.load(PipelineTestEnv.class);
                Iterator<PipelineTestEnv> iter=load.iterator();
                if(!iter.hasNext())
                    throw new IllegalStateException("No PipelineTestEnv found!");
                testEnv = iter.next();
                if(iter.hasNext())
                    throw new IllegalStateException("Only one PipelineTestEnv is allowed!");
            }
        }
        return testEnv;
    }
}
