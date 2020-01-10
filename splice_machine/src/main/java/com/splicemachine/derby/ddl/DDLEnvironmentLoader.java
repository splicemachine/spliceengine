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
