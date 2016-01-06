package com.splicemachine.derby.lifecycle;

import com.splicemachine.SqlEnvironment;
import com.splicemachine.tools.version.SpliceMachineVersion;
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


    public static SqlEnvironment loadEnvironment(Snowflake snowflake,
                                                 Connection internalConnection,
                                                 SpliceMachineVersion spliceVersion){
        SqlEnvironment env = sqlEnv;
        if(env==null){
            env = initializeEnvironment(snowflake,internalConnection,spliceVersion);
        }
        return env;
    }

    private static synchronized SqlEnvironment initializeEnvironment(Snowflake snowflake,
                                                                     Connection internalConnection,
                                                                     SpliceMachineVersion spliceVersion){
        SqlEnvironment env = sqlEnv;
        if(env==null){
            ServiceLoader<SqlEnvironment> load = ServiceLoader.load(SqlEnvironment.class);
            Iterator<SqlEnvironment> iter = load.iterator();
            if(!iter.hasNext())
                throw new IllegalStateException("No SITestEnv found!");
            env = sqlEnv = iter.next();
            sqlEnv.initialize(snowflake,internalConnection,spliceVersion);
            if(iter.hasNext())
                throw new IllegalStateException("Only one SITestEnv is allowed!");
        }
        return env;
    }
}
