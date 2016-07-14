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

import java.sql.Connection;
import java.sql.SQLException;

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.backup.BackupManager;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.PropertyManagerService;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.impl.sql.HSqlExceptionFactory;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.management.DatabaseAdministrator;
import com.splicemachine.management.JmxDatabaseAdminstrator;
import com.splicemachine.management.Manager;
import com.splicemachine.olap.AsyncOlapNIOLayer;
import com.splicemachine.olap.JobExecutor;
import com.splicemachine.olap.TimedOlapClient;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.uuid.Snowflake;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class HEngineSqlEnv extends EngineSqlEnvironment{

    private PropertyManager propertyManager;
    private PartitionLoadWatcher loadWatcher;
    private DataSetProcessorFactory processorFactory;
    private SqlExceptionFactory exceptionFactory;
    private DatabaseAdministrator dbAdmin;
    private OlapClient olapClient;

    @Override
    public void initialize(SConfiguration config,
                           Snowflake snowflake,
                           Connection internalConnection,
                           DatabaseVersion spliceVersion){
        super.initialize(config,snowflake,internalConnection,spliceVersion);
        this.propertyManager =PropertyManagerService.loadPropertyManager();
        this.loadWatcher = HBaseRegionLoads.INSTANCE;
        SIDriver driver =SIDriver.driver();
        this.processorFactory = new CostChoosingDataSetProcessorFactory();
        this.exceptionFactory = new HSqlExceptionFactory(SIDriver.driver().getExceptionFactory());
        this.dbAdmin = new JmxDatabaseAdminstrator();
        this.olapClient = initializeOlapClient(config,driver.getClock());
    }

    @Override
    public DatabaseAdministrator databaseAdministrator(){
        return dbAdmin;
    }

    @Override
    public SqlExceptionFactory exceptionFactory(){
        return exceptionFactory;
    }

    @Override
    public Manager getManager(){
        return ManagerLoader.load();
    }

    @Override
    public PartitionLoadWatcher getLoadWatcher(){
        return loadWatcher;
    }

    @Override
    public DataSetProcessorFactory getProcessorFactory(){
        return processorFactory;
    }

    @Override
    public OlapClient getOlapClient() {
        return olapClient;
    }

    @Override
    public PropertyManager getPropertyManager(){
        return propertyManager;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private OlapClient initializeOlapClient(SConfiguration config,Clock clock) {
        int timeoutMillis = config.getOlapClientWaitTime();
        int port = config.getOlapServerBindPort();
        HBaseConnectionFactory hbcf = HBaseConnectionFactory.getInstance(config);
        String host;
        try {
            host = hbcf.getMasterServer().getHostname();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        JobExecutor onl = new AsyncOlapNIOLayer(host,port);
        return new TimedOlapClient(onl,timeoutMillis);
    }

    @Override
    public void refreshEnterpriseFeatures() {
        ManagerLoader.clear();
    }
}
