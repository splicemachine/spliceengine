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

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

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
import com.splicemachine.olap.OlapServerProvider;
import com.splicemachine.olap.TimedOlapClient;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.uuid.Snowflake;
import org.apache.log4j.Logger;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class HEngineSqlEnv extends EngineSqlEnvironment{
    private static final Logger LOG = Logger.getLogger(HEngineSqlEnv.class);

    // MAX_EXECUTOR_CORES is calculated the first time someone runs a query.
    // numNodes is written to zookeeper by OlapServerMaster, so we have
    // to wait until the Olap Server comes up before getting numNodes from zookeeper.
    private static int MAX_EXECUTOR_CORES = -1;
    // A value to use until the Olap Server comes up.
    private static int DEFAULT_MAX_EXECUTOR_CORES = 8;
    private static int numSparkNodes = -1;

    private PropertyManager propertyManager;
    private PartitionLoadWatcher loadWatcher;
    private DataSetProcessorFactory processorFactory;
    private SqlExceptionFactory exceptionFactory;
    private DatabaseAdministrator dbAdmin;
    private OlapClient olapClient;
    private OperationManager operationManager;
    private ZkServiceDiscovery serviceDiscovery;
    private static final String maxExecutorCoresZkPath =
            HConfiguration.getConfiguration().getSpliceRootPath() + HBaseConfiguration.MAX_EXECUTOR_CORES;


    // Find maxExecutorCores from Zookeeper.
    private static byte [] getMaxExecutorCoresBytes() {
        byte [] returnData = null;
        try {
            byte [] data = ZkUtils.getData(maxExecutorCoresZkPath);
            if (data != null && data.length > 0)
                returnData = data;
        }
        catch (Exception | java.lang.AssertionError e) {
            LOG.warn("Unable to find maxExecutorCores in zookeeper.");
        }
        return returnData;
    }

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


    @Override
    public int getMaxExecutorCores() {
        if (MAX_EXECUTOR_CORES != -1)
            return MAX_EXECUTOR_CORES;
        synchronized (HEngineSqlEnv.class) {
            byte [] maxExecutorCoresBytes = getMaxExecutorCoresBytes();
            if (maxExecutorCoresBytes == null)
                return DEFAULT_MAX_EXECUTOR_CORES;

            int maxExecutorCores;
            int numNodes;
            try {
                ByteArrayInputStream bis = new ByteArrayInputStream(maxExecutorCoresBytes);
                ObjectInput in = new ObjectInputStream(bis);
                maxExecutorCores = in.readInt();
                numNodes = in.readInt();
            }
            catch (Exception e) {
                return DEFAULT_MAX_EXECUTOR_CORES;
            }

            if (numNodes > 0 && maxExecutorCores > 0) {
                numSparkNodes = numNodes;
                MAX_EXECUTOR_CORES = maxExecutorCores;
            }
            else
                return DEFAULT_MAX_EXECUTOR_CORES;
        }
        return MAX_EXECUTOR_CORES;
    }

    /**
     * Estimate the number of input splits used to read a table
     * <code>tableSize</code> bytes in size, assuming a splits
     * hint is not used.
     *
     * @return The number of input splits Splice would use to
     * read a table of <code>tableSize</code> bytes, with
     * <code>numRegions</code> HBase regions, via Spark.
     */
    @Override
    public int getNumSplits(long tableSize, int numRegions) {
        int numSplits, minSplits = 0;
        if (tableSize < 0)
            tableSize = 0;

        long bytesPerSplit = HConfiguration.getConfiguration().getSplitBlockSize();
        minSplits = HConfiguration.getConfiguration().getSplitsPerRegionMin();
        if (numRegions > 0)
            minSplits *= numRegions;
        if ((tableSize / bytesPerSplit) > Integer.MAX_VALUE)
            numSplits = Integer.MAX_VALUE;
        else
            numSplits = (int)(tableSize / bytesPerSplit);

        if (numSplits < minSplits)
            numSplits = minSplits;

        return numSplits;
    }
}
