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

package com.splicemachine.derby.lifecycle;

import com.splicemachine.SqlEnvironment;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.uuid.Snowflake;

import java.sql.Connection;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class SqlEnvironmentLoader{
    private static volatile SqlEnvironment sqlEnv;


    public static SqlEnvironment loadEnvironment(SConfiguration config,
                                                 Snowflake snowflake,
                                                 Connection internalConnection,
                                                 DatabaseVersion spliceVersion){
        SqlEnvironment env = sqlEnv;
        if(env==null){
            env = initializeEnvironment(config,snowflake,internalConnection,spliceVersion);
        }
        return env;
    }

    private static synchronized SqlEnvironment initializeEnvironment(SConfiguration config,
                                                                     Snowflake snowflake,
                                                                     Connection internalConnection,
                                                                     DatabaseVersion spliceVersion){
        SqlEnvironment env = sqlEnv;
        if(env==null){
            ServiceLoader<SqlEnvironment> load = ServiceLoader.load(SqlEnvironment.class);
            Iterator<SqlEnvironment> iter = load.iterator();
            if(!iter.hasNext())
                throw new IllegalStateException("No SqlEnvironment found!");
            env = sqlEnv = iter.next();
            sqlEnv.initialize(config,snowflake,internalConnection,spliceVersion);
            if(iter.hasNext())
                throw new IllegalStateException("Only one SqlEnvironment is allowed!");
        }
        return env;
    }
}
