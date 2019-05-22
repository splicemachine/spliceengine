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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.olap.AsyncOlapNIOLayer;
import com.splicemachine.olap.JobExecutor;
import com.splicemachine.olap.OlapServerProvider;
import com.splicemachine.olap.TimedOlapClient;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.uuid.Snowflake;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import static java.lang.System.getProperty;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class HEngineSqlEnv extends EngineSqlEnvironment{
    private static final Logger LOG = Logger.getLogger(HEngineSqlEnv.class);

    // Calculate this once on startup to save some overhead.
    private static final int MAX_EXECUTOR_CORES = calculateMaxExecutorCores();

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
        int maxRetries = config.getOlapServerMaxRetries();
        HBaseConnectionFactory hbcf = HBaseConnectionFactory.getInstance(config);
        final OlapServerProvider osp = queue -> {
            try {
                if (config.getOlapServerExternal()) {
                    String serverName = hbcf.getMasterServer().getServerName();
                    byte[] bytes = null;
                    int tries = 0;
                    IOException catched = null;
                    while (tries < maxRetries) {
                        tries++;
                        try {
                            bytes = ZkUtils.getData(HConfiguration.getConfiguration().getSpliceRootPath() +
                                    HBaseConfiguration.OLAP_SERVER_PATH + "/" + serverName + ":" + queue);
                            break;
                        } catch (IOException e) {
                            catched = e;
                            if (e.getCause() instanceof KeeperException.NoNodeException) {
                                // sleep & retry
                                try {
                                    long pause = PipelineUtils.getPauseTime(tries, 10);
                                    LOG.warn("Couldn't find OlapServer znode after " + tries+ " retries, sleeping for " +pause + " ms", e);
                                    clock.sleep(pause, TimeUnit.MILLISECONDS);
                                } catch (InterruptedException ie) {
                                    throw new IOException(ie);
                                }
                            } else {
                                throw e;
                            }
                        }
                    }
                    if (bytes == null)
                        throw catched;
                    String hostAndPort = Bytes.toString(bytes);
                    return HostAndPort.fromString(hostAndPort);
                } else {
                    return HostAndPort.fromParts(hbcf.getMasterServer().getHostname(), config.getOlapServerBindPort());
                }
            } catch (SQLException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException)
                    throw (IOException) cause;
                else
                    throw new IOException(e);
            }
        };

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

    /**
     * Parse a Spark or Hadoop size parameter value, that may use k, m, g or t
     * to represent kilobytes, megabytes, gigabytes or terabytes, respectively,
     * and return back the corresponding number of bytes.
     * @param sizeString the parameter value string to parse
     * @param defaultValue the default value of the parameter if an invalid
     *                     <code>sizeString</code> was passed.
     * @return The value in bytes of <code>sizeString</code>, or <code>defaultValue</code>
     *         if a <code>sizeString</code> was passed that could not be parsed.
     */
    private static long parseSizeString(String sizeString, long defaultValue) {
        long retVal = defaultValue;
        Pattern sizePattern = Pattern.compile("([\\d.]+)([kmgt])", Pattern.CASE_INSENSITIVE);
        Matcher matcher = sizePattern.matcher(sizeString);
        Map<String, Integer> suffixes = new HashMap<>();
        suffixes.put("k", 1);
        suffixes.put("m", 2);
        suffixes.put("g", 3);
        suffixes.put("t", 4);
        if (matcher.find()) {
            BigInteger value;
            String digits = matcher.group(1);
            try {
              value = new BigInteger(digits);
            }
            catch (NumberFormatException e) {
              return defaultValue;
            }
            int power = suffixes.get(matcher.group(2).toLowerCase());
            BigInteger multiplicand = BigInteger.valueOf(1024).pow(power);
            value = value.multiply(multiplicand);
            if (value.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0)
              return Long.MAX_VALUE;
            if (value.compareTo(BigInteger.valueOf(1)) < 0)
              return defaultValue;

            retVal = value.longValue();
        }
        else {
            try {
                retVal = Long.valueOf(sizeString);
            }
            catch (NumberFormatException e) {
                return defaultValue;
            }
            if (retVal < 1)
                retVal = defaultValue;
        }
        return retVal;
    }

    @Override
    public int getMaxExecutorCores() {
        return MAX_EXECUTOR_CORES;
    }

    /**
     * Estimate the maximum number of Spark executor cores that could be simultaneously
     * be running given the current YARN and splice.spark settings.
     *
     * @return The maximum number of Spark executor cores.
     */
    private static int calculateMaxExecutorCores() {
        String memorySize = HConfiguration.unwrapDelegate().get("yarn.nodemanager.resource.memory-mb");
        String sparkDynamicAllocationString = getProperty("splice.spark.dynamicAllocation.enabled");
        String executorInstancesString = getProperty("splice.spark.executor.instances");
        String executorCoresString = getProperty("splice.spark.executor.cores");
        String sparkExecutorMemory = getProperty("splice.spark.executor.memory");

        int executorCores = 1;
        if (executorCoresString != null) {
            try {
                executorCores = Integer.valueOf(executorCoresString);
                if (executorCores < 1)
                    executorCores = 1;
            } catch (NumberFormatException e) {
            }
        }

        // Initialize to defaults, then check for custom settings.
        long memSize = 8192*1024*1024, containerSize;
        if (memorySize != null) {
            try {
                memSize = Long.valueOf(memorySize) * 1024 * 1024;
            }
            catch (NumberFormatException e) {
            }
        }

        boolean dynamicAllocation = sparkDynamicAllocationString != null &&
                                    Boolean.valueOf(sparkDynamicAllocationString).booleanValue();
        int numSparkExecutorCores = executorCores;

        int numSparkExecutors = 0;
        if (!dynamicAllocation) {
            try {
                numSparkExecutors = 1;
                numSparkExecutors = Integer.valueOf(executorInstancesString);
                if (numSparkExecutors < 1)
                    numSparkExecutors = 1;
                numSparkExecutorCores = numSparkExecutors * executorCores;

            } catch (NumberFormatException e) {
            }
        }
        else {
            String sparkDynamicAllocationMaxExecutors = getProperty("splice.spark.dynamicAllocation.maxExecutors");
            numSparkExecutors = 0;
            if (sparkDynamicAllocationMaxExecutors != null) {
                try {
                    numSparkExecutors = Integer.valueOf(executorInstancesString);
                }
                catch(NumberFormatException e){
                }
            }
        }

        long executorMemory = 1024 * 1024 * 1024; // 1g
        if (sparkExecutorMemory != null)
            executorMemory = parseSizeString(sparkExecutorMemory, executorMemory);
        if (numSparkExecutors != 0)
            executorMemory /= numSparkExecutors;

        containerSize = executorMemory;

        int maxExecutorsSupportedByYARN =
            (memSize / containerSize) > Integer.MAX_VALUE ?
                                        Integer.MAX_VALUE : (int)(memSize / containerSize);

        if (maxExecutorsSupportedByYARN < 1)
            maxExecutorsSupportedByYARN = 1;

        int maxExecutorCoresSupportedByYARN = maxExecutorsSupportedByYARN * executorCores;

        if (dynamicAllocation)
            if (numSparkExecutors < 1)
                numSparkExecutorCores = maxExecutorCoresSupportedByYARN;
            else
                numSparkExecutorCores = numSparkExecutors * executorCores;

        if (numSparkExecutorCores > maxExecutorCoresSupportedByYARN)
            numSparkExecutorCores = maxExecutorCoresSupportedByYARN;


        return numSparkExecutorCores;
    }

    /**
     * Estimate the number of input splits used to read a table
     * <code>tableSize</code> bytes in size, assuming a splits
     * hint is not used.
     *
     * @return The number of input splits Splice would use to
     * read a table of <code>tableSize</code> bytes via Spark.
     */
    @Override
    public int getNumSplits(long tableSize) {
        int numSplits, minSplits = 0;
        if (tableSize < 0)
            tableSize = 0;

        long bytesPerSplit = HConfiguration.getConfiguration().getSplitBlockSize();
        minSplits = HConfiguration.getConfiguration().getSplitsPerRegionMin();
        if ((tableSize / bytesPerSplit) > Integer.MAX_VALUE)
            numSplits = Integer.MAX_VALUE;
        else
            numSplits = (int)(tableSize / bytesPerSplit);

        if (numSplits < minSplits)
            numSplits = minSplits;

        return numSplits;
    }
}
