package com.splicemachine.derby.ddl;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class DDLEnvironmentLoader{
    public static volatile DDLEnvironment INSTANCE;

    private DDLEnvironmentLoader(){}

    public static DDLEnvironment loadEnvironment(){
        DDLEnvironment env = INSTANCE;
        if(env==null){
            synchronized(DDLEnvironment.class){
                env = INSTANCE;
                if(env==null){
                    env = INSTANCE = loadEnvironmentService();
                    DDLDriver.loadDriver(env);
                }
            }
        }

        return env;
    }

    private static DDLEnvironment loadEnvironmentService(){
        ServiceLoader<DDLEnvironment> load=ServiceLoader.load(DDLEnvironment.class);
        Iterator<DDLEnvironment> iter=load.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No DDL Environment found!");
        DDLEnvironment env = iter.next();
        if(iter.hasNext())
            throw new IllegalStateException("Only one DDL Environment is allowed!");
        return env;
    }
}
