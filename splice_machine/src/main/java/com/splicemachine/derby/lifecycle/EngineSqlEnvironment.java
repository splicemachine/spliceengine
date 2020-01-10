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

package com.splicemachine.derby.lifecycle;

import com.splicemachine.SqlEnvironment;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.ServiceDiscovery;
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
