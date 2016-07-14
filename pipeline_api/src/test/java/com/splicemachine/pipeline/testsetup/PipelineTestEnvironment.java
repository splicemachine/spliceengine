/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.pipeline.testsetup;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class PipelineTestEnvironment{
    private static volatile PipelineTestEnv testEnv;
    private static volatile PipelineTestDataEnv testDataEnv;

    public static PipelineTestEnv loadTestEnvironment(){
        PipelineTestEnv env = testEnv;
        if(env==null){
            env = initializeFullEnvironment();
        }
        return env;
    }

    public static PipelineTestDataEnv loadTestDataEnvironment(){
        PipelineTestDataEnv dataEnv = testDataEnv;
        if(dataEnv==null){
            dataEnv = testEnv;
            if(dataEnv==null){
                dataEnv = initializeDataEnvironment();
            }
        }
        return dataEnv;
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
                testDataEnv = testEnv;
                if(iter.hasNext())
                    throw new IllegalStateException("Only one PipelineTestEnv is allowed!");
            }
        }
        return testEnv;
    }

    private static PipelineTestDataEnv initializeDataEnvironment(){
        synchronized(PipelineTestEnvironment.class){
            if(testDataEnv==null){
                if(testEnv==null){
                    ServiceLoader<PipelineTestDataEnv> load=ServiceLoader.load(PipelineTestDataEnv.class);
                    Iterator<PipelineTestDataEnv> iter=load.iterator();
                    if(!iter.hasNext())
                        throw new IllegalStateException("No PipelineTestDataEnv found!");
                    testDataEnv = iter.next();
                    if(iter.hasNext())
                        throw new IllegalStateException("Only one PipelineTestDataEnv is allowed!");

                }else{
                    testDataEnv = testEnv;
                }
            }
        }
        return testDataEnv;
    }
}
