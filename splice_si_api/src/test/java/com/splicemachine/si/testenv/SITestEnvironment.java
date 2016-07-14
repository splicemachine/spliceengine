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

package com.splicemachine.si.testenv;

import java.io.IOException;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class SITestEnvironment{
    private static volatile SITestEnv testEnv;
    private static volatile SITestDataEnv testDataEnv;

    public static SITestDataEnv loadTestDataEnvironment(){
        SITestDataEnv env = testDataEnv;
        if(env==null){
            env = testEnv;
            if(env==null){
                env = initializeDataEnvironment();
            }
        }
        return env;
    }


    public static SITestEnv loadTestEnvironment() throws IOException{
        if(testEnv==null)
            initializeFullEnvironment();

        return testEnv;
    }

    private static void initializeFullEnvironment() throws IOException{
        synchronized(SITestEnvironment.class){
            if(testEnv==null){
                ServiceLoader<SITestEnv> load=ServiceLoader.load(SITestEnv.class);
                Iterator<SITestEnv> iter=load.iterator();
                if(!iter.hasNext())
                    throw new IllegalStateException("No SITestEnv found!");
                testDataEnv = testEnv = iter.next();
                testEnv.initialize();
                if(iter.hasNext())
                    throw new IllegalStateException("Only one SITestEnv is allowed!");
            }
        }
    }

    private static SITestDataEnv initializeDataEnvironment(){
        synchronized(SITestEnvironment.class){
            if(testDataEnv==null){
               ServiceLoader<SITestDataEnv> load = ServiceLoader.load(SITestDataEnv.class);
                Iterator<SITestDataEnv> iter=load.iterator();
                if(!iter.hasNext())
                    throw new IllegalStateException("No SITestEnv found!");
                testDataEnv = iter.next();
                if(iter.hasNext())
                    throw new IllegalStateException("Only one SITestEnv is allowed!");
            }
        }
        return testDataEnv;
    }

}
