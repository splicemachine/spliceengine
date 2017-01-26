/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
