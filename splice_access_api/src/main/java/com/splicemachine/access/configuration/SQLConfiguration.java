/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.access.configuration;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class SQLConfiguration implements ConfigurationDefault {
    public static final String SPLICE_DB = "splicedb";
    public static final String SPLICE_USER = "SPLICE";
    public static final String SPLICE_JDBC_DRIVER = "com.splicemachine.db.jdbc.ClientDriver";
    public static final String CONGLOMERATE_TABLE_NAME =SIConfigurations.CONGLOMERATE_TABLE_NAME;
    @SuppressFBWarnings(value = "MS_MUTABLE_ARRAY",justification = "Intentional")
    public static final byte[] CONGLOMERATE_TABLE_NAME_BYTES = SIConfigurations.CONGLOMERATE_TABLE_NAME_BYTES;

    //TODO -sf- move this to HBase-specific configuration
    public static final String PARTITIONSERVER_PORT="hbase.regionserver.port";
    private static int DEFAULT_PARTITIONSERVER_PORT=16020;
    /**
     * The IP address to bind the Derby connection to.
     * Defaults to 0.0.0.0
     */
    public static final String NETWORK_BIND_ADDRESS= "splice.server.address";
    public static final String DEFAULT_NETWORK_BIND_ADDRESS= "0.0.0.0";

    /**
     * The Port to bind the Derby connection to.
     * Defaults to 1527
     */
    public static final String NETWORK_BIND_PORT= "splice.server.port";
    public static final int DEFAULT_NETWORK_BIND_PORT= 1527;

    /**
     * Ignore SavePts flag for experimental TPCC testing.
     */
    public static final String IGNORE_SAVE_POINTS = "splice.ignore.savepts";
    public static final boolean DEFAULT_IGNORE_SAVEPTS = false;

    /**
     *
     * This is where install_jar places the jar files...  Needs to be configured for stand alone versions to know
     * where to look for jar files.
     *
     */
    public static final String STORAGE_FACTORY_HOME = "splice.storage.factory.home";

    public static final String IMPORT_MAX_QUOTED_COLUMN_LINES="splice.import.maxQuotedColumnLines";
    private static final int DEFAULT_IMPORT_MAX_QUOTED_COLUMN_LINES = 50000;

    public static final String BATCH_ONCE_BATCH_SIZE = "splice.batchonce.batchsize";
    private static final int DEFAULT_BATCH_ONCE_BATCH_SIZE = 50_000;

    public static final String CONTROL_SIDE_COST_THRESHOLD = "splice.dataset.control.costThreshold";
    private static final double DEFAULT_CONTROL_SIDE_COST_THRESHOLD = 1000000D;

    public static final String CONTROL_SIDE_ROWCOUNT_THRESHOLD = "splice.dataset.control.rowCountThreshold";
    private static final double DEFAULT_CONTROL_SIDE_ROWCOUNT_THRESHOLD = 100000D;

    //debug options
    /**
     * For debugging an operation, this will force the query parser to dump any generated
     * class files to the HBASE_HOME directory. This is not useful for anything except
     * debugging certain kinds of errors, and is NOT recommended enabled in a production
     * environment.
     *
     * Defaults to false (off)
     */
    public static final String DEBUG_DUMP_CLASS_FILE = "splice.debug.dumpClassFile";
    private static final boolean DEFAULT_DUMP_CLASS_FILE=false;

    public static final String DEBUG_DUMP_BIND_TREE = "splice.debug.compile.dumpBindTree";
    private static final boolean DEFAULT_DUMP_BIND_TREE=false;

    public static final String DEBUG_DUMP_OPTIMIZED_TREE = "splice.debug.compile.dumpOptimizedTree";
    private static final boolean DEFAULT_DUMP_OPTIMIZED_TREE=false;

    /**
     * For logging sql statements.  This is off by default. Turning on will hurt you in the case of an OLTP workload.
     */
    public static final String DEBUG_LOG_STATEMENT_CONTEXT = "splice.debug.logStatementContext";
    private static final boolean DEFAULT_LOG_STATEMENT_CONTEXT=false;

    /**
     * Threshold in megabytes for the broadcast join region size.
     *
     */
    public static final String BROADCAST_REGION_MB_THRESHOLD = "splice.optimizer.broadcastRegionMBThreshold";
    private static final int DEFAULT_BROADCAST_REGION_MB_THRESHOLD = (int) (Runtime.getRuntime().maxMemory() / (1024l * 1024l * 100l));

    /**
     * Threshold in rows for the broadcast join region size.  Default is 1 Million Rows
     *
     */
    public static final String BROADCAST_REGION_ROW_THRESHOLD = "splice.optimizer.broadcastRegionRowThreshold";
    private static final int DEFAULT_BROADCAST_REGION_ROW_THRESHOLD = 1000000;

    /**
     * Minimum fixed duration (in millisecomds) that should be allowed to lapse
     * before the optimizer can determine that it should stop trying to find
     * the best plan due to plan time taking longer than the expected
     * query execution time. By default, this is zero, which means
     * there is no fixed minimum, and the determination is made
     * using cost estimates alone. Default value should generally
     * be left alone, and would only need to be changed as a workaround
     * for inaccurate cost estimates.
     *
     */
    public static final String OPTIMIZER_PLAN_MINIMUM_TIMEOUT = "splice.optimizer.minPlanTimeout";
    private static final long DEFAULT_OPTIMIZER_PLAN_MINIMUM_TIMEOUT = 0L;

    /**
     * Maximum fixed duration (in millisecomds) that should be allowed to lapse
     * before the optimizer can determine that it should stop trying to find
     * the best plan due to plan time taking longer than the expected
     * query execution time. By default, this is Long.MaxValue, which means
     * there is no fixed maximum, and the determination is made
     * using cost estimates alone. Default value should generally
     * be left alone, and would only need to be changed as a workaround
     * for inaccurate cost estimates.
     *
     */
    public static final String OPTIMIZER_PLAN_MAXIMUM_TIMEOUT = "splice.optimizer.maxPlanTimeout";
    private static final long DEFAULT_OPTIMIZER_PLAN_MAXIMUM_TIMEOUT = Long.MAX_VALUE;

    /**
     * The maximum number of Kryo objects to pool for reuse. This setting is generally
     * not necessary to adjust unless there are an extremely large number of concurrent
     * operations allowed on the system. Adjusting this down may lengthen the amount of
     * time required to perform an operation slightly.
     *
     * Defaults to 50.
     */
    public static final String KRYO_POOL_SIZE = "splice.marshal.kryoPoolSize";
    private static final int DEFAULT_KRYO_POOL_SIZE=16000;

    /**
     * Flag to force the upgrade process to execute during database boot-up.
     * This flag should only be true for the master server.  If the upgrade runs on the region server,
     * it would probably be bad (at least if it ran concurrently with another upgrade).
     * On region servers, this flag will be temporarily true until the SpliceDriver is started.
     * The SpliceDriver will set the flag to false for all region servers.
     * Default is false.
     */
    public static final String UPGRADE_FORCED = "splice.upgrade.forced";
    private static final boolean DEFAULT_UPGRADE_FORCED = false;

    /**
     * If the upgrade process is being forced, this tells which version to begin the upgrade process from.
     * Default is "0.0.0", which means that all upgrades will be executed.
     */
    public static final String UPGRADE_FORCED_FROM = "splice.upgrade.forced.from";
    private static final String DEFAULT_UPGRADE_FORCED_FROM = "0.0.0";

    /**
     * The number of index rows to bulk fetch at a single time.
     *
     * Index lookups are bundled together into a single network operation for many rows.
     * This setting determines the maximum number of rows which are fetched in a single
     * network operation.
     *
     * Defaults to 4000
     */
    public static final String INDEX_BATCH_SIZE = "splice.index.batchSize";
    private static final int DEFAULT_INDEX_BATCH_SIZE=4000;


    public static volatile boolean upgradeForced = false;

    /**
     * The number of concurrent bulk fetches a single index operation can initiate
     * at a time. If fewer than that number of fetches are currently in progress, the
     * index operation will submit a new bulk fetch. Once this setting's number of bulk
     * fetches has been reached, the index lookup must wait for one of the previously
     * submitted fetches to succeed before continuing.
     *
     * Index lookups will only submit a new bulk fetch if existing data is not already
     * available.
     *
     * Defaults to 5
     */
    public static final String INDEX_LOOKUP_BLOCKS = "splice.index.numConcurrentLookups";
    private static final int DEFAULT_INDEX_LOOKUP_BLOCKS = 5;

    public static final String PARTITIONSERVER_JMX_PORT = "hbase.regionserver.jmx.port";
    private static final int DEFAULT_PARTITIONSERVER_JMX_PORT = 10102;

    public static final String NESTEDLOOPJOIN_BATCH_SIZE = "splice.nestedLoopJoin.batchSize";
    private static final int DEFAULT_NESTEDLOOPJOIN_BATCH_SIZE = 10;

    @Override
    public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        // FIXME: JC - some of these are not referenced anywhere outside. Do we need them?

        builder.networkBindPort = configurationSource.getInt(NETWORK_BIND_PORT, DEFAULT_NETWORK_BIND_PORT);
        builder.kryoPoolSize = configurationSource.getInt(KRYO_POOL_SIZE, DEFAULT_KRYO_POOL_SIZE);
        builder.indexBatchSize = configurationSource.getInt(INDEX_BATCH_SIZE, DEFAULT_INDEX_BATCH_SIZE);
        builder.indexLookupBlocks = configurationSource.getInt(INDEX_LOOKUP_BLOCKS, DEFAULT_INDEX_LOOKUP_BLOCKS);
        builder.importMaxQuotedColumnLines = configurationSource.getInt(IMPORT_MAX_QUOTED_COLUMN_LINES, DEFAULT_IMPORT_MAX_QUOTED_COLUMN_LINES);
        builder.batchOnceBatchSize = configurationSource.getInt(BATCH_ONCE_BATCH_SIZE, DEFAULT_BATCH_ONCE_BATCH_SIZE);
        builder.partitionserverJmxPort = configurationSource.getInt(PARTITIONSERVER_JMX_PORT, DEFAULT_PARTITIONSERVER_JMX_PORT);
        builder.partitionserverPort = configurationSource.getInt(PARTITIONSERVER_PORT, DEFAULT_PARTITIONSERVER_PORT);
        builder.nestedLoopJoinBatchSize = configurationSource.getInt(NESTEDLOOPJOIN_BATCH_SIZE, DEFAULT_NESTEDLOOPJOIN_BATCH_SIZE);

        // Where to place jar files...
        String defaultStorageFactoryHome;
        if (System.getProperty("hbase.rootdir") != null)
            defaultStorageFactoryHome = System.getProperty("hbase.rootdir");
        else
            defaultStorageFactoryHome = configurationSource.getString("hbase.rootdir",System.getProperty("hbase.rootdir"));
        builder.storageFactoryHome = configurationSource.getString(STORAGE_FACTORY_HOME,defaultStorageFactoryHome);
        builder.optimizerPlanMaximumTimeout = configurationSource.getLong(OPTIMIZER_PLAN_MAXIMUM_TIMEOUT, DEFAULT_OPTIMIZER_PLAN_MAXIMUM_TIMEOUT);
        builder.optimizerPlanMinimumTimeout = configurationSource.getLong(OPTIMIZER_PLAN_MINIMUM_TIMEOUT, DEFAULT_OPTIMIZER_PLAN_MINIMUM_TIMEOUT);
        builder.broadcastRegionMbThreshold = configurationSource.getLong(BROADCAST_REGION_MB_THRESHOLD, DEFAULT_BROADCAST_REGION_MB_THRESHOLD);
        builder.broadcastRegionRowThreshold = configurationSource.getLong(BROADCAST_REGION_ROW_THRESHOLD, DEFAULT_BROADCAST_REGION_ROW_THRESHOLD);

        //always disable debug statements by default
        builder.debugLogStatementContext = configurationSource.getBoolean(DEBUG_LOG_STATEMENT_CONTEXT, DEFAULT_LOG_STATEMENT_CONTEXT);
        builder.debugDumpBindTree = configurationSource.getBoolean(DEBUG_DUMP_BIND_TREE, DEFAULT_DUMP_BIND_TREE);
        builder.debugDumpOptimizedTree = configurationSource.getBoolean(DEBUG_DUMP_OPTIMIZED_TREE, DEFAULT_DUMP_OPTIMIZED_TREE);
        builder.debugDumpClassFile = configurationSource.getBoolean(DEBUG_DUMP_CLASS_FILE, DEFAULT_DUMP_CLASS_FILE);
        builder.ignoreSavePoints = configurationSource.getBoolean(IGNORE_SAVE_POINTS, DEFAULT_IGNORE_SAVEPTS);
        builder.upgradeForced = configurationSource.getBoolean(UPGRADE_FORCED, DEFAULT_UPGRADE_FORCED);

//        builder.controlSideCostThreshold = configurationSource.getDouble(CONTROL_SIDE_COST_THRESHOLD, DEFAULT_CONTROL_SIDE_COST_THRESHOLD);
//        builder.controlSideRowcountThreshold = configurationSource.getDouble(CONTROL_SIDE_ROWCOUNT_THRESHOLD, DEFAULT_CONTROL_SIDE_ROWCOUNT_THRESHOLD);

        builder.networkBindAddress = configurationSource.getString(NETWORK_BIND_ADDRESS, DEFAULT_NETWORK_BIND_ADDRESS);
    }
}
