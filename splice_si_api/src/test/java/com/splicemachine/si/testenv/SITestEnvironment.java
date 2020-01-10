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
