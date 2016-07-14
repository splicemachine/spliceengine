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

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.backup.BackupManager;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.impl.sql.*;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.derby.impl.sql.NoOpManager;
import com.splicemachine.management.DatabaseAdministrator;
import com.splicemachine.management.DirectDatabaseAdministrator;
import com.splicemachine.management.Manager;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.uuid.Snowflake;

import java.sql.Connection;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MEngineSqlEnv extends EngineSqlEnvironment{

    private PropertyManager propertyManager;
    private PartitionLoadWatcher partitionLoadWatcher;
    private DataSetProcessorFactory dspFactory;
    private SqlExceptionFactory exceptionFactory;
    private DatabaseAdministrator dbAdmin;

    @Override
    public void initialize(SConfiguration config,
                           Snowflake snowflake,
                           Connection internalConnection,
                           DatabaseVersion spliceVersion){
        super.initialize(config,snowflake,internalConnection,spliceVersion);
        this.propertyManager = new DirectPropertyManager();
        this.partitionLoadWatcher = new DirectPartitionLoadWatcher();
        this.dspFactory = new ControlOnlyDataSetProcessorFactory();
        this.exceptionFactory = new MSqlExceptionFactory(SIDriver.driver().getExceptionFactory());
        this.dbAdmin = new DirectDatabaseAdministrator();
    }

    @Override
    public SqlExceptionFactory exceptionFactory(){
        return exceptionFactory;
    }

    @Override
    public DatabaseAdministrator databaseAdministrator(){
        return dbAdmin;
    }

    @Override
    public OlapClient getOlapClient() {
        return LocalOlapClient.getInstance();
    }

    @Override
    public Manager getManager(){
        return NoOpManager.getInstance();
    }

    @Override
    public PartitionLoadWatcher getLoadWatcher(){
        return partitionLoadWatcher;
    }

    @Override
    public DataSetProcessorFactory getProcessorFactory(){
        return dspFactory;
    }

    @Override
    public PropertyManager getPropertyManager(){
        return propertyManager;
    }
}
