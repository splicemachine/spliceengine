/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
