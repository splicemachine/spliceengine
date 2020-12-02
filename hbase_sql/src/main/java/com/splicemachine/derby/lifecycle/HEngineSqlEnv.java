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

import java.math.BigInteger;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import splice.com.google.common.net.HostAndPort;
import com.splicemachine.SqlExceptionFactory;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.ServiceDiscovery;
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
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.uuid.Snowflake;
import org.apache.log4j.Logger;

import static com.splicemachine.access.configuration.HBaseConfiguration.SPARK_NUM_NODES_PATH;
import static java.lang.System.getProperty;

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
    private static int numSparkNodes = -1;

    private PropertyManager propertyManager;
    private PartitionLoadWatcher loadWatcher;
    private DataSetProcessorFactory processorFactory;
    private SqlExceptionFactory exceptionFactory;
    private DatabaseAdministrator dbAdmin;
    private OlapClient olapClient;
    private OperationManager operationManager;
    private ZkServiceDiscovery serviceDiscovery;
    private static final String sparkNumNodesZkPath =
            HConfiguration.getConfiguration().getSpliceRootPath() + SPARK_NUM_NODES_PATH;


    // Find the number of nodes on which Spark executors can be run.
    private static int getNumSparkNodes() {
        int numNodes = -1;
        try {
            byte [] data = ZkUtils.getData(sparkNumNodesZkPath);
            if (data != null && data.length > 0)
                numNodes = Bytes.toInt(data);
        }
        catch (Exception | java.lang.AssertionError e) {
            LOG.warn("Unable to find the number of spark nodes from zookeeper.");
        }
        return numNodes;
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


    /**
     * Parse a Spark or Hadoop size parameter value, that may use b, k, m, g, t, p
     * to represent bytes, kilobytes, megabytes, gigabytes, terabytes or petabytes respectively,
     * and return back the corresponding number of bytes.  Valid suffixes can also end
     * with a 'b' : kb, mb, gb, tb, pb.
     * @param sizeString the parameter value string to parse
     * @param defaultValue the default value of the parameter if an invalid
     *                     <code>sizeString</code> was passed.
     * @return The value in bytes of <code>sizeString</code>, or <code>defaultValue</code>
     *         if a <code>sizeString</code> was passed that could not be parsed.
     */
    public static long parseSizeString(String sizeString, long defaultValue, String defaultSuffix) {
        long retVal = defaultValue;
        Pattern sizePattern = Pattern.compile("(^[\\d.]+)([bkmgtp]$)", Pattern.CASE_INSENSITIVE);
        Pattern sizePattern2 = Pattern.compile("(^[\\d.]+)([kmgtp][b]$)", Pattern.CASE_INSENSITIVE);
        sizeString = sizeString.trim();

        // Add a default suffix if none is specified.
        if (sizeString.matches("^.*\\d$"))
            sizeString = sizeString + defaultSuffix;

        Matcher matcher1 = sizePattern.matcher(sizeString);
        Matcher matcher2 = sizePattern2.matcher(sizeString);
        Map<String, Integer> suffixes = new HashMap<>();
        suffixes.put("b", 0);
        suffixes.put("k", 1);
        suffixes.put("m", 2);
        suffixes.put("g", 3);
        suffixes.put("t", 4);
        suffixes.put("p", 5);
        suffixes.put("kb", 1);
        suffixes.put("mb", 2);
        suffixes.put("gb", 3);
        suffixes.put("tb", 4);
        suffixes.put("pb", 5);

        boolean found1 = matcher1.find();
        boolean found2 = matcher2.find();
        Matcher matcher = found1 ? matcher1 : matcher2;

        if (found1 || found2) {
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
            if (value.compareTo(BigInteger.valueOf(0)) < 0)
              return defaultValue;

            retVal = value.longValue();
        }
        else {
            try {
                retVal = Long.parseLong(sizeString);
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
        if (MAX_EXECUTOR_CORES != -1)
            return MAX_EXECUTOR_CORES;
        synchronized (HEngineSqlEnv.class) {
            int sparkNodes = getNumSparkNodes();
            int maxExecutorCores =
              calculateMaxExecutorCores(HConfiguration.unwrapDelegate().get("yarn.nodemanager.resource.memory-mb"),
                                        getProperty("splice.spark.dynamicAllocation.enabled"),
                                        getProperty("splice.spark.executor.instances"),
                                        getProperty("splice.spark.executor.cores"),
                                        getProperty("splice.spark.executor.memory"),
                                        getProperty("splice.spark.dynamicAllocation.maxExecutors"),
                                        getProperty("splice.spark.executor.memoryOverhead"),
                                        getProperty("splice.spark.yarn.executor.memoryOverhead"),
                                        sparkNodes > 0 ? sparkNodes : 1);
            if (sparkNodes > 0) {
                numSparkNodes = sparkNodes;
                MAX_EXECUTOR_CORES = maxExecutorCores;
            }
        }
        return MAX_EXECUTOR_CORES;
    }

    /**
     * Estimate the maximum number of Spark executor cores that could be running
     * simultaneously given the current YARN and splice.spark settings.
     *
     * @return The maximum number of Spark executor cores.
     * @notes See @link https://spark.apache.org/docs/latest/configuration.html
     *        for more information on spark configuration properties.
     */
    public static int
    calculateMaxExecutorCores(String memorySize,                         // yarn.nodemanager.resource.memory-mb
                              String sparkDynamicAllocationString,       // splice.spark.dynamicAllocation.enabled
                              String executorInstancesString,            // splice.spark.executor.instances
                              String executorCoresString,                // splice.spark.executor.cores
                              String sparkExecutorMemory,                // splice.spark.executor.memory
                              String sparkDynamicAllocationMaxExecutors, // splice.spark.dynamicAllocation.maxExecutors
                              String sparkExecutorMemoryOverhead,        // splice.spark.executor.memoryOverhead
                              String sparkYARNExecutorMemoryOverhead,    // splice.spark.yarn.executor.memoryOverhead
                              int numNodes)
    {
        if (numNodes < 1)
            numNodes = 1;

        int executorCores = 1;
        if (executorCoresString != null) {
            try {
                executorCores = Integer.parseInt(executorCoresString);
                if (executorCores < 1)
                    executorCores = 1;
            } catch (NumberFormatException e) {
            }
        }

        // Initialize to defaults, then check for custom settings.
        long memSize = 8192L*1024*1024, containerSize;
        if (memorySize != null) {
            try {
                memSize = Long.parseLong(memorySize) * 1024 * 1024;
            }
            catch (NumberFormatException e) {
            }
        }

        boolean dynamicAllocation = sparkDynamicAllocationString != null &&
                                    Boolean.parseBoolean(sparkDynamicAllocationString);
        int numSparkExecutorCores = executorCores;

        int numSparkExecutors = 0;
        if (!dynamicAllocation) {
            try {
                numSparkExecutors = 1;
                numSparkExecutors = Integer.parseInt(executorInstancesString);
                if (numSparkExecutors < 1)
                    numSparkExecutors = 1;
                numSparkExecutorCores = numSparkExecutors * executorCores;

            } catch (NumberFormatException e) {
            }
        }
        else {
            numSparkExecutors = Integer.MAX_VALUE;
            if (sparkDynamicAllocationMaxExecutors != null) {
                try {
                    numSparkExecutors = Integer.parseInt(sparkDynamicAllocationMaxExecutors);
                }
                catch(NumberFormatException e){
                }
            }
        }

        long executorMemory = 1024L * 1024 * 1024; // 1g
        if (sparkExecutorMemory != null)
            executorMemory =
                parseSizeString(sparkExecutorMemory, executorMemory, "b");

        long sparkMemOverhead = executorMemory / 10;
        if (sparkExecutorMemoryOverhead != null) {
            try {
                sparkMemOverhead =
                    parseSizeString(sparkExecutorMemoryOverhead, sparkMemOverhead, "m");
            } catch (NumberFormatException e) {
            }
        }
        long sparkMemOverheadLegacy = executorMemory / 10;
        if (sparkYARNExecutorMemoryOverhead != null) {
            try {
                sparkMemOverheadLegacy =
                    parseSizeString(sparkYARNExecutorMemoryOverhead, sparkMemOverheadLegacy, "m");
            } catch (NumberFormatException e) {
            }
        }
        sparkMemOverhead = Math.max(sparkMemOverheadLegacy, sparkMemOverhead);

        // Total executor memory includes the memory overhead.
        containerSize = executorMemory + sparkMemOverhead;

        int maxExecutorsSupportedByYARN =
            (memSize / containerSize) > Integer.MAX_VALUE ?
                                        Integer.MAX_VALUE : (int)(memSize / containerSize);

        if (maxExecutorsSupportedByYARN < 1)
            maxExecutorsSupportedByYARN = 1;

        maxExecutorsSupportedByYARN *= numNodes;

        int maxExecutorCoresSupportedByYARN = maxExecutorsSupportedByYARN * executorCores;

        if (dynamicAllocation) {
            if (numSparkExecutors < 1)
                numSparkExecutors = 1;
            numSparkExecutorCores = ((long)numSparkExecutors * executorCores) > Integer.MAX_VALUE ?
                                     Integer.MAX_VALUE : numSparkExecutors * executorCores;
        }

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
