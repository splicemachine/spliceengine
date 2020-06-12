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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.google.common.net.HostAndPort;
import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.ServiceDiscovery;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.derby.iapi.sql.PartitionLoadWatcher;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.derby.iapi.sql.PropertyManagerService;
import com.splicemachine.derby.iapi.sql.execute.DataSetProcessorFactory;
import com.splicemachine.derby.iapi.sql.execute.OperationManager;
import com.splicemachine.derby.iapi.sql.execute.OperationManagerImpl;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.impl.sql.HSqlExceptionFactory;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.hbase.ZkServiceDiscovery;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.management.DatabaseAdministrator;
import com.splicemachine.management.JmxDatabaseAdminstrator;
import com.splicemachine.management.Manager;
import com.splicemachine.olap.AsyncOlapNIOLayer;
import com.splicemachine.olap.JobExecutor;
import com.splicemachine.olap.OlapServerNotReadyException;
import com.splicemachine.olap.OlapServerProvider;
import com.splicemachine.olap.OlapServerZNode;
import com.splicemachine.olap.TimedOlapClient;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.uuid.Snowflake;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class HEngineSqlEnv extends EngineSqlEnvironment{
    private static final Logger LOG = Logger.getLogger(HEngineSqlEnv.class);

    private PropertyManager propertyManager;
    private PartitionLoadWatcher loadWatcher;
    private DataSetProcessorFactory processorFactory;
    private SqlExceptionFactory exceptionFactory;
    private DatabaseAdministrator dbAdmin;
    private OlapClient olapClient;
    private OperationManager operationManager;
    private ZkServiceDiscovery serviceDiscovery;

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
        this.operationManager = new OperationManagerImpl();
        this.serviceDiscovery = new ZkServiceDiscovery();
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
        final int retries = config.getOlapClientRetries();
        HBaseConnectionFactory hbcf = HBaseConnectionFactory.getInstance(config);
        final OlapServerProvider osp = new OlapServerProviderImpl(config, clock, hbcf);

        Stream.Builder<String> queuesBuilder = Stream.builder();
        queuesBuilder.accept(SIConstants.OLAP_DEFAULT_QUEUE_NAME);
        if (config.getOlapServerIsolatedCompaction()) {
            queuesBuilder.accept(config.getOlapServerIsolatedCompactionQueueName());
        }
        Stream<String> queues = Stream.concat(config.getOlapServerIsolatedRoles().values().stream(),
                                              queuesBuilder.build());
        Map<String, JobExecutor> executorMap = new HashMap<>();
        queues.forEach(queue -> {
            JobExecutor onl = new AsyncOlapNIOLayer(osp, queue, retries);
            executorMap.put(queue, onl);
        });

        return new TimedOlapClient(executorMap,timeoutMillis);
    }

    @Override
    public void refreshEnterpriseFeatures() {
        ManagerLoader.clear();
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
