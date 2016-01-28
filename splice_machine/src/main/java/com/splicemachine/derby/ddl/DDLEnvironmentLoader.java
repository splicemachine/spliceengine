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
