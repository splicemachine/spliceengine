/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.ServiceDiscovery;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.execute.OperationManager;
import com.splicemachine.derby.iapi.sql.execute.OperationManagerImpl;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.impl.sql.*;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.management.DatabaseAdministrator;
import com.splicemachine.management.DirectDatabaseAdministrator;
import com.splicemachine.management.Manager;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.uuid.Snowflake;
import org.spark_project.guava.net.HostAndPort;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;

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
    private OperationManager operationManager;
    private ServiceDiscovery serviceDiscovery;

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
        this.operationManager = new OperationManagerImpl();
        this.serviceDiscovery = new ServiceDiscovery() {
            @Override
            public void registerServer(HostAndPort hostAndPort) throws IOException {
                // no-op
            }

            @Override
            public void deregisterServer(HostAndPort hostAndPort) throws IOException {
                // no-op
            }

            @Override
            public List<HostAndPort> listServers() throws IOException {
                return Arrays.asList(HostAndPort.fromParts(InetAddress.getLocalHost().getHostName(), config.getNetworkBindPort()));
            }
        };
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

    @Override
    public void refreshEnterpriseFeatures() {
        // No Op
    }

    @Override
    public OperationManager getOperationManager() {
        return operationManager;
    }

    @Override
    public ServiceDiscovery serviceDiscovery() {
        return serviceDiscovery;
    }
}
