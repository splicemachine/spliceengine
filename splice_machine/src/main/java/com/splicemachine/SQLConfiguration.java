package com.splicemachine;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.SIConfigurations;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class SQLConfiguration{
    public static final String SPLICE_DB = "splicedb";
    public static final String SPLICE_USER = "SPLICE";
    public static final String SPLICE_JDBC_DRIVER = "com.splicemachine.db.jdbc.ClientDriver";
    public static final String CONGLOMERATE_TABLE_NAME =SIConfigurations.CONGLOMERATE_TABLE_NAME;
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

    public static final String IMPORT_MAX_QUOTED_COLUMN_LINES="splice.import.maxQuotedColumnLines";
    private static final int DEFAULT_IMPORT_MAX_QUOTED_COLUMN_LINES = 50000;

    public static final String BATCH_ONCE_BATCH_SIZE = "splice.batchonce.batchsize";
    private static final int DEFAULT_BATCH_ONCE_BATCH_SIZE = 50_000;

    public static final String CONTROL_SIDE_COST_THRESHOLD = "splice.dataset.control.costThreshold";
    private static final double DEFAULT_CONTROL_SIDE_COST_THRESHOLD = 10*1000*1000*1000d; // based on a TPCC1000 run on an 8 node cluster

    public static final String CONTROL_SIDE_ROWCOUNT_THRESHOLD = "splice.dataset.control.rowCountThreshold";
    private static final double DEFAULT_CONTROL_SIDE_ROWCOUNT_THRESHOLD = 1E10;

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

    public static final SConfiguration.Defaults defaults=new SConfiguration.Defaults(){
        @Override
        public long defaultLongFor(String key){
            switch(key){
                case OPTIMIZER_PLAN_MAXIMUM_TIMEOUT: return DEFAULT_OPTIMIZER_PLAN_MAXIMUM_TIMEOUT;
                case OPTIMIZER_PLAN_MINIMUM_TIMEOUT: return DEFAULT_OPTIMIZER_PLAN_MINIMUM_TIMEOUT;
                case BROADCAST_REGION_MB_THRESHOLD: return DEFAULT_BROADCAST_REGION_MB_THRESHOLD;
                case BROADCAST_REGION_ROW_THRESHOLD: return DEFAULT_BROADCAST_REGION_ROW_THRESHOLD;
                default:
                    throw new IllegalArgumentException("No long default for key '"+key+"'");
            }
        }

        @Override
        public boolean hasLongDefault(String key){
            switch(key){
                case OPTIMIZER_PLAN_MAXIMUM_TIMEOUT:
                case OPTIMIZER_PLAN_MINIMUM_TIMEOUT:
                case BROADCAST_REGION_MB_THRESHOLD:
                case BROADCAST_REGION_ROW_THRESHOLD:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public int defaultIntFor(String key){
            switch(key){
                case NETWORK_BIND_PORT: return DEFAULT_NETWORK_BIND_PORT;
                case KRYO_POOL_SIZE: return DEFAULT_KRYO_POOL_SIZE;
                case INDEX_BATCH_SIZE: return DEFAULT_INDEX_BATCH_SIZE;
                case INDEX_LOOKUP_BLOCKS: return DEFAULT_INDEX_LOOKUP_BLOCKS;
                case IMPORT_MAX_QUOTED_COLUMN_LINES: return DEFAULT_IMPORT_MAX_QUOTED_COLUMN_LINES;
                case BATCH_ONCE_BATCH_SIZE: return DEFAULT_BATCH_ONCE_BATCH_SIZE;
                case PARTITIONSERVER_JMX_PORT: return DEFAULT_PARTITIONSERVER_JMX_PORT;
                case PARTITIONSERVER_PORT: return DEFAULT_PARTITIONSERVER_PORT;
                default:
                    throw new IllegalArgumentException("No SQL default for key '"+key+"'");
            }
        }

        @Override
        public boolean hasIntDefault(String key){
            switch(key){
                case NETWORK_BIND_PORT:
                case KRYO_POOL_SIZE:
                case INDEX_BATCH_SIZE:
                case INDEX_LOOKUP_BLOCKS:
                case IMPORT_MAX_QUOTED_COLUMN_LINES:
                case BATCH_ONCE_BATCH_SIZE:
                case PARTITIONSERVER_JMX_PORT:
                case PARTITIONSERVER_PORT:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public boolean hasStringDefault(String key){
            switch(key){
                case DEBUG_LOG_STATEMENT_CONTEXT:
                case NETWORK_BIND_ADDRESS:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public String defaultStringFor(String key){
            switch(key){
                case NETWORK_BIND_ADDRESS: return DEFAULT_NETWORK_BIND_ADDRESS;
                default:
                    throw new IllegalArgumentException("No SQL default for key '"+key+"'");
            }
        }

        @Override
        public boolean defaultBooleanFor(String key){
            switch(key){
                case DEBUG_LOG_STATEMENT_CONTEXT:
                case DEBUG_DUMP_BIND_TREE:
                case DEBUG_DUMP_CLASS_FILE:
                case DEBUG_DUMP_OPTIMIZED_TREE:
                    return false; //always disable debug statements by default
                case IGNORE_SAVE_POINTS: return DEFAULT_IGNORE_SAVEPTS;
                default:
                    return false;
            }
        }

        @Override
        public boolean hasBooleanDefault(String key){
            switch(key){
                case DEBUG_LOG_STATEMENT_CONTEXT:
                case DEBUG_DUMP_BIND_TREE:
                case DEBUG_DUMP_CLASS_FILE:
                case DEBUG_DUMP_OPTIMIZED_TREE:
                case IGNORE_SAVE_POINTS:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public double defaultDoubleFor(String key){
            switch(key){
                case CONTROL_SIDE_COST_THRESHOLD:
                    return DEFAULT_CONTROL_SIDE_COST_THRESHOLD;
                case CONTROL_SIDE_ROWCOUNT_THRESHOLD:
                    return DEFAULT_CONTROL_SIDE_ROWCOUNT_THRESHOLD;
                default:
                    throw new IllegalArgumentException("No SQL default for key '"+key+"'");
            }
        }

        @Override
        public boolean hasDoubleDefault(String key){
            switch(key){
                case CONTROL_SIDE_COST_THRESHOLD:
                case CONTROL_SIDE_ROWCOUNT_THRESHOLD:
                    return true;
                default:
                    return false;
            }
        }
    };
}
