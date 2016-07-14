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

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public abstract class EngineSqlEnvironment implements SqlEnvironment{

    private SConfiguration config;
    private Snowflake snowflake;
    private Connection connection;
    private DatabaseVersion version;

    public EngineSqlEnvironment(){
    }

    @Override
    public void initialize(SConfiguration config,Snowflake snowflake,Connection internalConnection,DatabaseVersion spliceVersion){
        this.snowflake = snowflake;
        this.connection = internalConnection;
        this.version = spliceVersion;
        this.config = config;
    }

    @Override
    public Snowflake getUUIDGenerator(){
        return snowflake;
    }

    @Override
    public Connection getInternalConnection(){
        return connection;
    }

    @Override
    public DatabaseVersion getVersion(){
        return version;
    }

    @Override
    public SConfiguration getConfiguration(){
        return config;
    }
}
