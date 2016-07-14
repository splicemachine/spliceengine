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

package com.splicemachine.derby.ddl;

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.api.SConfiguration;

import java.io.IOException;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class DDLEnvironmentLoader{
    public static volatile DDLEnvironment INSTANCE;

    private DDLEnvironmentLoader(){}

    public static DDLEnvironment loadEnvironment(SConfiguration config,SqlExceptionFactory exceptionFactory) throws IOException{
        DDLEnvironment env = INSTANCE;
        if(env==null){
            synchronized(DDLEnvironment.class){
                env = INSTANCE;
                if(env==null){
                    env = INSTANCE = loadEnvironmentService(exceptionFactory,config);
                    DDLDriver.loadDriver(env);
                }
            }
        }

        return env;
    }

    private static DDLEnvironment loadEnvironmentService(SqlExceptionFactory exceptionFactory,SConfiguration config) throws IOException{
        ServiceLoader<DDLEnvironment> load=ServiceLoader.load(DDLEnvironment.class);
        Iterator<DDLEnvironment> iter=load.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No DDL Environment found!");
        DDLEnvironment env = iter.next();
        env.configure(exceptionFactory,config);
        if(iter.hasNext())
            throw new IllegalStateException("Only one DDL Environment is allowed!");
        return env;
    }
}
