package com.splicemachine.constants;

import java.util.List;

import com.splicemachine.encoding.Encoding;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.common.collect.Lists;

public class SpliceConstants {
    // Splice Configuration
	public static Configuration config = SpliceConfiguration.create();

	// Zookeeper Default Paths
	public static final String DEFAULT_BASE_TASK_QUEUE_NODE = "/spliceTasks";
    public static final String DEFAULT_BASE_JOB_QUEUE_NODE = "/spliceJobs";
    public static final String DEFAULT_TRANSACTION_PATH = "/transactions";
	public static final String DEFAULT_CONGLOMERATE_SCHEMA_PATH = "/conglomerates";
	public static final String DEFAULT_DERBY_PROPERTY_PATH = "/derbyPropertyPath";
	public static final String DEFAULT_QUERY_NODE_PATH = "/queryNodePath";
	public static final String DEFAULT_STARTUP_PATH = "/startupPath";	
	public static final String DEFAULT_LEADER_ELECTION = "/leaderElection";	

    /* Derby configuration settings */

    //Derby network configuration
    /**
     * The IP address to bind the Derby connection to. Defaults to 0.0.0.0
     */
    public static final String CONFIG_DERBY_BIND_ADDRESS = "splice.server.address";
    public static final String DEFAULT_DERBY_BIND_ADDRESS = "0.0.0.0";

    /**
     * The Port to bind the Derby connection to. Defaults to 0.0.0.0
     */
    public static final String CONFIG_DERBY_BIND_PORT = "splice.server.port";
    public static final int DEFAULT_DERBY_BIND_PORT = 1527;

    /*Task and Job management*/

    /**
     * The priority under which to run user operation tasks. This can be any positive number, the higher
     * the priority, the sooner operations will be executed, relative to other prioritized tasks (such
     * as imports, TEMP cleaning, etc.)
     */
    public static final String CONFIG_OPERATION_PRIORITY = "splice.task.operationPriority";
    public static final int DEFAULT_IMPORT_TASK_PRIORITY = 3;
    /**
     * The Priority with which to assign import tasks. Setting this number higher than the
     * operation priority will make imports run preferentially to operation tasks; setting it lower
     * will make operations run preferentially to import tasks.
     */
    public static final String CONFIG_IMPORT_TASK_PRIORITY = "splice.task.importTaskPriority";
    public static final int DEFAULT_OPERATION_PRIORITY = 3;

    //common SI fields
    public static final String NA_TRANSACTION_ID = "NA_TRANSACTION_ID";
    public static final String SI_EXEMPT = "si-exempt";

    /*Writer configuration*/
	// Constants
    public static final int DEFAULT_POOL_MAX_SIZE = Integer.MAX_VALUE;
    public static final int DEFAULT_POOL_CORE_SIZE = 100;
    public static final long DEFAULT_POOL_CLEANER_INTERVAL = 60;
    public static final int DEFAULT_MAX_PENDING_BUFFERS = 10;
    public static final long DEFAULT_CACHE_UPDATE_PERIOD = 30000;
    public static final long DEFAULT_CACHE_EXPIRATION = 60;
    public static final long DEFAULT_WRITE_BUFFER_SIZE = 2097152;
    public static final int DEFAULT_MAX_BUFFER_ENTRIES = 1000;
    public static final int DEFAULT_HBASE_HTABLE_THREADS_MAX = Integer.MAX_VALUE;
    public static final int DEFAULT_HBASE_HTABLE_THREADS_CORE = 10;
    public static final long DEFAULT_HBASE_HTABLE_THREADS_KEEPALIVETIME = 60;
    public static final int DEFAULT_HBASE_CLIENT_RETRIES_NUMBER = HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER;
    public static final boolean DEFAULT_HBASE_CLIENT_COMPRESS_WRITES = false;
	public static final String DEFAULT_COMPRESSION = "none";
	public static final String DEFAULT_MULTICAST_GROUP_ADDRESS = "230.0.0.1";
	public static final int DEFAULT_MULTICAST_GROUP_PORT = 4446;
	public static final int DEFAULT_RMI_PORT = 40001;
	public static final int DEFAULT_RMI_REMOTE_OBJECT_PORT = 47000;
    public static final int DEFAULT_STARTUP_LOCK_PERIOD=1000;
    public static final int DEFAULT_RING_BUFFER_SIZE=1000;
    public static final int DEFAULT_INDEX_BATCH_SIZE=500;
    public static final int DEFAULT_INDEX_BUFFER_SIZE=1000;
    public static final int DEFAULT_KRYO_POOL_SIZE=50;



	/**
	 * The Default Cache size for Scans.
	 *
	 * This determines the default number of rows that will be cached on each scan returned.
	 */
	public static final int DEFAULT_CACHE_SIZE = 100;

    
    /*
     * Setting the cache update interval <0 indicates that caching is to be turned off.
     * This is a performance killer, but is useful when debugging issues.
     */

    
	
	// Zookeeper Path Configuration Constants
	public static final String CONFIG_BASE_TASK_QUEUE_NODE = "splice.task_queue_node";
    public static final String CONFIG_BASE_JOB_QUEUE_NODE = "splice.job_queue_node";
    public static final String CONFIG_TRANSACTION_PATH = "splice.transactions_node";
	public static final String CONFIG_CONGLOMERATE_SCHEMA_PATH = "splice.conglomerates_node";
	public static final String CONFIG_DERBY_PROPERTY_PATH = "splice.derby_property_node";
	public static final String CONFIG_QUERY_NODE_PATH = "splice.query_node_path";
	public static final String CONFIG_STARTUP_PATH = "splice.startup_path";
	public static final String CONFIG_LEADER_ELECTION = "splice.leader_election";
    private static final String CONFIG_POOL_MAX_SIZE = "splice.table.pool.maxsize";
    private static final String CONFIG_POOL_CORE_SIZE = "splice.table.pool.coresize";
    private static final String CONFIG_POOL_CLEANER_INTERVAL = "splice.table.pool.cleaner.interval";
    private static final String CONFIG_WRITE_BUFFER_SIZE = "hbase.client.write.buffer";
    public static final String CONFIG_WRITE_BUFFER_MAX_FLUSHES = "hbase.client.write.buffers.maxflushes";
    private static final String CONFIG_BUFFER_ENTRIES = "hbase.client.write.buffer.maxentries";
    private static final String CONFIG_HBASE_HTABLE_THREADS_MAX = "hbase.htable.threads.max";
    private static final String CONFIG_HBASE_HTABLE_THREADS_CORE = "hbase.htable.threads.core";
    public static final String CONFIG_HBASE_HTABLE_THREADS_KEEPALIVETIME = "hbase.htable.threads.keepalivetime";
    public static final String CONFIG_HBASE_CLIENT_RETRIES_NUMBER = HConstants.HBASE_CLIENT_RETRIES_NUMBER;
    public static final String CONFIG_CACHE_UPDATE_PERIOD = "hbase.htable.regioncache.updateinterval";
    public static final String CONFIG_CACHE_EXPIRATION = "hbase.htable.regioncache.expiration";
    public static final String CONFIG_HBASE_CLIENT_COMPRESS_WRITES = "hbase.client.compress.writes";
    public static final String CONFIG_COMPRESSION = "splice.compression";
    public static final String CONFIG_MULTICAST_GROUP_ADDRESS = "splice.multicast_group_address";
    public static final String CONFIG_MULTICAST_GROUP_PORT = "splice.multicast_group_port";
    public static final String CONFIG_RMI_PORT = "splice.rmi_port";
    public static final String CONFIG_RMI_REMOTE_OBJECT_PORT = "splice.rmi_remote_object_port";

    private static final String DEBUG_DUMP_CLASS_FILE = "splice.debug.dumpClassFile";
    private static final String DEBUG_FAIL_TASKS_RANDOMLY = "splice.debug.failTasksRandomly";
    private static final String DEBUG_TASK_FAILURE_RATE = "splice.debug.taskFailureRate";

    private static final String STARTUP_LOCK_WAIT_PERIOD = "splice.startup.lockWaitPeriod";
    private static final String RING_BUFFER_SIZE = "splice.ring.bufferSize";
    private static final String INDEX_BATCH_SIZE = "splice.index.batchSize";
    private static final String INDEX_BUFFER_SIZE = "splice.index.bufferSize";
    private static final String KRYO_POOL_SIZE = "splice.marshal.kryoPoolSize";

    private static final String SEQUENCE_BLOCK_SIZE = "splice.sequence.allocationBlockSize";
    private static final int DEFAULT_SEQUENCE_BLOCK_SIZE = 1000;
    
	// Zookeeper Actual Paths
	public static String zkSpliceTaskPath;
	public static String zkSpliceJobPath;
	public static String zkSpliceTransactionPath;
	public static String zkSpliceConglomeratePath;
	public static String zkSpliceConglomerateSequencePath;
	public static String zkSpliceDerbyPropertyPath;
	public static String zkSpliceQueryNodePath;
	public static String zkSpliceStartupPath;
	public static String zkLeaderElection;
	public static String derbyBindAddress;
	public static int derbyBindPort;
    public static int operationTaskPriority;
    public static int importTaskPriority;
	public static Long sleepSplitInterval;
	public static int tablePoolMaxSize;
	public static int tablePoolCoreSize;
	public static long tablePoolCleanerInterval;
	public static long writeBufferSize;
	public static int maxBufferEntries;
	public static int maxThreads;
    public static int coreWriteThreads;
    public static int maxTreeThreads; //max number of threads for concurrent stack execution
	public static long threadKeepAlive;
    public static int numRetries;
    public static boolean enableRegionCache;
    public static long cacheExpirationPeriod;
    public static boolean compressWrites;
    public static String compression;
    public static String multicastGroupAddress;
    public static int multicastGroupPort;
    public static int rmiPort;
    public static int rmiRemoteObjectPort;
    public static int startupLockWaitPeriod;
    public static int ringBufferSize;
    public static int indexBatchSize;
    public static int indexBufferSize;
    public static int kryoPoolSize;

    public static long maxRollForwardHeapSize;
    public static int maxRollForwardEntries;
    public static long rollForwardTimeout;
    public static int maxRollForwardScheduledThreads;
    public static int maxRollForwardLoadThreads;
    public static int maxRollForwardCoreThreads;
    public static int maxConcurrentRollForwards;

    /*Used to determine how many sequence numbers to reserve in a given block*/
    public static long sequenceBlockSize;

    
    /*
     * Setting the cache update interval <0 indicates that caching is to be turned off.
     * This is a performance killer, but is useful when debugging issues.
     */
    public static long cacheUpdatePeriod;

	
	
	// Splice Internal Tables
    public static final String TEMP_TABLE = "SPLICE_TEMP";
    public static final String TEST_TABLE = "SPLICE_TEST";
    public static final String TRANSACTION_TABLE = "SPLICE_TXN";
    public static final String CONGLOMERATE_TABLE_NAME = "SPLICE_CONGLOMERATE";
    public static final String SEQUENCE_TABLE_NAME = "SPLICE_SEQUENCES";
    public static final String PROPERTIES_TABLE_NAME = "SPLICE_PROPS";
    public static final String PROPERTIES_CACHE = "properties";
    public static final String SYSSCHEMAS_CACHE = "SYSSCHEMAS_CACHE";
    public static final String SYSSCHEMAS_INDEX1_ID_CACHE = "SYSSCHEMAS_INDEX1_ID_CACHE";
    public static final String[] SYSSCHEMAS_CACHES = {SYSSCHEMAS_CACHE,SYSSCHEMAS_INDEX1_ID_CACHE};
    
    public static byte[] TEMP_TABLE_BYTES = Bytes.toBytes(TEMP_TABLE);
    public static final byte[] TRANSACTION_TABLE_BYTES = Bytes.toBytes(TRANSACTION_TABLE);
    public static final byte[] CONGLOMERATE_TABLE_NAME_BYTES = Bytes.toBytes(CONGLOMERATE_TABLE_NAME);
    public static final byte[] SEQUENCE_TABLE_NAME_BYTES = Bytes.toBytes(SEQUENCE_TABLE_NAME);
    public static final byte[]PROPERTIES_TABLE_NAME_BYTES = Bytes.toBytes(PROPERTIES_TABLE_NAME);
    
	// Splice Family Information
	public static final String DEFAULT_FAMILY = "V";
	public static final byte[] DEFAULT_FAMILY_BYTES = Bytes.toBytes(DEFAULT_FAMILY);

    //TEMP Table task column--used for filtering out failed tasks from the temp
    //table

	// Splice Default Table Definitions
	public static final int DEFAULT_VERSIONS = HColumnDescriptor.DEFAULT_VERSIONS;

	public static final Boolean DEFAULT_IN_MEMORY = HColumnDescriptor.DEFAULT_IN_MEMORY;
	public static final Boolean DEFAULT_BLOCKCACHE=HColumnDescriptor.DEFAULT_BLOCKCACHE;
	public static final int DEFAULT_TTL = HColumnDescriptor.DEFAULT_TTL;
	public static final String DEFAULT_BLOOMFILTER = HColumnDescriptor.DEFAULT_BLOOMFILTER;
		
	public static final String TABLE_COMPRESSION = "com.splicemachine.table.compression";
	public static final String HBASE_ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
	public static final String HBASE_ZOOKEEPER_QUOROM = "hbase.zookeeper.quorum";

    // Default Constants
	public static final String SPACE = " ";
	public final static String PATH_DELIMITER = "/";
	public static final int FIELD_FLAGS_SIZE = 1;
    public static final byte[] EOF_MARKER = new byte[] {0, 0, 0, 0};
    public static final String SUPPRESS_INDEXING_ATTRIBUTE_NAME = "iu";
    public static final byte[] SUPPRESS_INDEXING_ATTRIBUTE_VALUE = new byte[]{};
    public static final byte[] VALUE_COLUMN = Encoding.encode(1);
	public static final long DEFAULT_SPLIT_WAIT_INTERVAL = 500l;
	public static final String SPLICE_DB = "spliceDB";

    public static final String ENTRY_PREDICATE_LABEL= "p";

    public static final boolean COLLECT_STATS = false;
	
    // Default Configuration Options
	public static final String SPLIT_WAIT_INTERVAL = "splice.splitWaitInterval";

    //debug options
    public static boolean dumpClassFile;
    public static final boolean DEFAULT_DUMP_CLASS_FILE=false;
    public static boolean debugFailTasksRandomly;
    public static final boolean DEFAULT_DEBUG_FAIL_TASKS_RANDOMLY=false;

    public static double debugTaskFailureRate;
    public static final double DEFAULT_DEBUG_TASK_FAILURE_RATE= 0.1; //fail 10% of tasks when enabled
    private static final String CONFIG_MAX_CONCURRENT_OPERATIONS = "splice.tree.maxConcurrentOperations";
    private static final int DEFAULT_MAX_CONCURRENT_OPERATIONS = 20; //probably too low

    private static final String ROLL_FORWARD_HEAP_SIZE = "splice.rollForward.maxHeapSize";
    private static final String ROLL_FORWARD_ENTRIES = "splice.rollForward.maxEntries";
    private static final String ROLL_FORWARD_TIMEOUT = "splice.rollForward.timeout";
    private static final String ROLL_FORWARD_MAX_SCHEDULED_THREADS = "splice.rollForward.maxScheduledThreads";
    private static final String ROLL_FORWARD_MAX_LOAD_THREADS = "splice.rollForward.maxLoadThreads";
    private static final String ROLL_FORWARD_LOAD_CORE_THREADS = "splice.rollForward.coreLoadThreads";
    private static final String ROLL_FORWARD_MAX_CONCURRENT_ROLLS = "splice.rollForward.maxConcurrentRollForwards";
    private static final long DEFAULT_ROLL_FORWARD_HEAP_SIZE = 2*1024*1024; //2 MB
    private static final int DEFAULT_ROLL_FORWARD_ENTRIES = 1000000; //1 million
    private static final long DEFAULT_ROLL_FORWARD_TIMEOUT = 10*1000; //10 seconds
    private static final int DEFAULT_ROLL_FORWARD_MAX_SCHEDULED_THREADS = 2;
    private static final int DEFAULT_ROLL_FORWARD_MAX_LOAD_THREADS = 10;
    private static final int DEFAULT_ROLL_FORWARD_LOAD_CORE_THREADS = 2;
    private static final int DEFAULT_ROLL_FORWARD_MAX_CONCURENT_ROLLS = 10; //at most 10*2MB = 20MB heap used at more for rollforwards

    private static final String IMPORT_MAX_PROCESSING_THREADS = "splice.import.maxProcessingThreads";
    private static final int DEFAULT_IMPORT_MAX_PROCESSING_THREADS = 3;
    public static int maxImportProcessingThreads;

    private static final String IMPORT_MAX_READ_BUFFER_SIZE = "splice.import.maxReadBufferSize";
    private static final int DEFAULT_IMPORT_MAX_READ_BUFFER_SIZE= 1000;
    public static int maxImportReadBufferSize;


    public static enum TableEnv {
    	TRANSACTION_TABLE,
    	ROOT_TABLE,
    	META_TABLE,
    	DERBY_SYS_TABLE,
    	USER_INDEX_TABLE,
    	USER_TABLE
    }

    public enum SpliceConglomerate {HEAP,BTREE}

	static {
		setParameters();
	}
	
	public static List<String> zookeeperPaths = Lists.newArrayList(zkSpliceTaskPath,zkSpliceJobPath,
			zkSpliceTransactionPath,zkSpliceConglomeratePath,zkSpliceConglomerateSequencePath,zkSpliceDerbyPropertyPath,zkSpliceQueryNodePath);

	public static List<byte[]> spliceSystables = Lists.newArrayList(TEMP_TABLE_BYTES,TRANSACTION_TABLE_BYTES,CONGLOMERATE_TABLE_NAME_BYTES);
	
	public static void setParameters() {
		zkSpliceTaskPath = config.get(CONFIG_BASE_TASK_QUEUE_NODE,DEFAULT_BASE_TASK_QUEUE_NODE);
		zkSpliceJobPath = config.get(CONFIG_BASE_JOB_QUEUE_NODE,DEFAULT_BASE_JOB_QUEUE_NODE);
		zkSpliceTransactionPath = config.get(CONFIG_TRANSACTION_PATH,DEFAULT_TRANSACTION_PATH);		
		zkSpliceConglomeratePath = config.get(CONFIG_CONGLOMERATE_SCHEMA_PATH,DEFAULT_CONGLOMERATE_SCHEMA_PATH);
		zkSpliceConglomerateSequencePath = zkSpliceConglomeratePath+"/__CONGLOM_SEQUENCE";
		zkSpliceDerbyPropertyPath = config.get(CONFIG_DERBY_PROPERTY_PATH,DEFAULT_DERBY_PROPERTY_PATH);
		zkSpliceQueryNodePath = config.get(CONFIG_CONGLOMERATE_SCHEMA_PATH,DEFAULT_CONGLOMERATE_SCHEMA_PATH);
		zkLeaderElection = config.get(CONFIG_LEADER_ELECTION,DEFAULT_LEADER_ELECTION);
		sleepSplitInterval = config.getLong(SPLIT_WAIT_INTERVAL, DEFAULT_SPLIT_WAIT_INTERVAL);
		zkSpliceStartupPath = config.get(CONFIG_STARTUP_PATH,DEFAULT_STARTUP_PATH);
        derbyBindAddress = config.get(CONFIG_DERBY_BIND_ADDRESS, DEFAULT_DERBY_BIND_ADDRESS);
        derbyBindPort = config.getInt(CONFIG_DERBY_BIND_PORT, DEFAULT_DERBY_BIND_PORT);
        operationTaskPriority = config.getInt(CONFIG_OPERATION_PRIORITY, DEFAULT_OPERATION_PRIORITY);
        importTaskPriority = config.getInt(CONFIG_IMPORT_TASK_PRIORITY, DEFAULT_IMPORT_TASK_PRIORITY);
        tablePoolMaxSize = config.getInt(CONFIG_POOL_MAX_SIZE,DEFAULT_POOL_MAX_SIZE);
        tablePoolCoreSize = config.getInt(CONFIG_POOL_CORE_SIZE, DEFAULT_POOL_CORE_SIZE);
        tablePoolCleanerInterval = config.getLong(CONFIG_POOL_CLEANER_INTERVAL, DEFAULT_POOL_CLEANER_INTERVAL);
        writeBufferSize = config.getLong(CONFIG_WRITE_BUFFER_SIZE, DEFAULT_WRITE_BUFFER_SIZE);
        maxBufferEntries = config.getInt(CONFIG_BUFFER_ENTRIES, DEFAULT_MAX_BUFFER_ENTRIES);
        maxThreads = config.getInt(CONFIG_HBASE_HTABLE_THREADS_MAX,DEFAULT_HBASE_HTABLE_THREADS_MAX);
        maxTreeThreads = config.getInt(CONFIG_MAX_CONCURRENT_OPERATIONS,DEFAULT_MAX_CONCURRENT_OPERATIONS);
        int ipcThreads = config.getInt("hbase.regionserver.handler.count",maxThreads);
        if(ipcThreads < maxThreads){
            /*
             * Some of our writes will also write out to indices and/or read data from HBase, which
             * may be located on the same region. Thus, if we allow unbounded writer threads, we face
             * a nasty situation where we are writing to a bunch of regions which are all located on the same
             * node, and they attempt to write out to indices which are ALSO on the same node. Since we are
             * using up all the IPC threads to do the initial writes, the writes out to the index tables are blocked.
             * But since the writes to the main table cannot complete before the index writes complete, the
             * main table writes cannot proceed, resulting in a deadlock (in pathological circumstances).
             *
             * This deadlock can be manually recovered from by moving regions around, but it's bad form to
             * deadlock periodically just because HBase isn't arranged nicely. Thus, we bound down the number
             * of write threads to be strictly less than the number of ipcThreads, so as to always leave some
             * IPC threads available (preventing deadlock).
             *
             * I more or less arbitrarily decided to make it 5 fewer, but that seems like a good balance
             * between having a lot of write threads and still allowing writes through.
             */
            maxThreads = ipcThreads-5;
        }
        if(maxThreads<=0)
            maxThreads = 1;
        coreWriteThreads = config.getInt(CONFIG_HBASE_HTABLE_THREADS_CORE,DEFAULT_HBASE_HTABLE_THREADS_MAX);
        if(coreWriteThreads>maxThreads){
            //default the core write threads to 10% of the maximum available
            coreWriteThreads = maxThreads/10;
        }
        if(coreWriteThreads<0)
            coreWriteThreads=0;

        threadKeepAlive = config.getLong(CONFIG_HBASE_HTABLE_THREADS_KEEPALIVETIME, DEFAULT_HBASE_HTABLE_THREADS_KEEPALIVETIME);
        numRetries = config.getInt(CONFIG_HBASE_CLIENT_RETRIES_NUMBER,DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
        cacheUpdatePeriod = config.getLong(CONFIG_CACHE_UPDATE_PERIOD,DEFAULT_CACHE_UPDATE_PERIOD);
        enableRegionCache = cacheUpdatePeriod>0l;
        cacheExpirationPeriod = config.getLong(CONFIG_CACHE_EXPIRATION,DEFAULT_CACHE_EXPIRATION);
        compressWrites = config.getBoolean(CONFIG_HBASE_CLIENT_COMPRESS_WRITES,DEFAULT_HBASE_CLIENT_COMPRESS_WRITES);
        compression = config.get(CONFIG_COMPRESSION, DEFAULT_COMPRESSION);
        multicastGroupAddress = config.get(CONFIG_MULTICAST_GROUP_ADDRESS,DEFAULT_MULTICAST_GROUP_ADDRESS);
        multicastGroupPort = config.getInt(CONFIG_MULTICAST_GROUP_PORT, DEFAULT_MULTICAST_GROUP_PORT);
        rmiPort = config.getInt(CONFIG_RMI_PORT, DEFAULT_RMI_PORT);
        rmiRemoteObjectPort = config.getInt(CONFIG_RMI_REMOTE_OBJECT_PORT, DEFAULT_RMI_REMOTE_OBJECT_PORT);
        dumpClassFile = config.getBoolean(DEBUG_DUMP_CLASS_FILE,DEFAULT_DUMP_CLASS_FILE);
        startupLockWaitPeriod = config.getInt(STARTUP_LOCK_WAIT_PERIOD,DEFAULT_STARTUP_LOCK_PERIOD);
        ringBufferSize = config.getInt(RING_BUFFER_SIZE, DEFAULT_RING_BUFFER_SIZE);
        indexBatchSize = config.getInt(INDEX_BATCH_SIZE,DEFAULT_INDEX_BATCH_SIZE);
        indexBufferSize = config.getInt(INDEX_BUFFER_SIZE,DEFAULT_INDEX_BUFFER_SIZE);
        kryoPoolSize = config.getInt(KRYO_POOL_SIZE,DEFAULT_KRYO_POOL_SIZE);
        debugFailTasksRandomly = config.getBoolean(DEBUG_FAIL_TASKS_RANDOMLY,DEFAULT_DEBUG_FAIL_TASKS_RANDOMLY);
        debugTaskFailureRate = config.getFloat(DEBUG_TASK_FAILURE_RATE,(float)DEFAULT_DEBUG_TASK_FAILURE_RATE);

        sequenceBlockSize = config.getInt(SEQUENCE_BLOCK_SIZE,DEFAULT_SEQUENCE_BLOCK_SIZE);

        maxRollForwardHeapSize = config.getLong(ROLL_FORWARD_HEAP_SIZE,DEFAULT_ROLL_FORWARD_HEAP_SIZE);
        maxRollForwardEntries = config.getInt(ROLL_FORWARD_ENTRIES, DEFAULT_ROLL_FORWARD_ENTRIES);
        rollForwardTimeout = config.getLong(ROLL_FORWARD_TIMEOUT, DEFAULT_ROLL_FORWARD_TIMEOUT);
        maxRollForwardScheduledThreads = config.getInt(ROLL_FORWARD_MAX_SCHEDULED_THREADS, DEFAULT_ROLL_FORWARD_MAX_SCHEDULED_THREADS);
        maxRollForwardLoadThreads = config.getInt(ROLL_FORWARD_MAX_LOAD_THREADS, DEFAULT_ROLL_FORWARD_MAX_LOAD_THREADS);
        maxRollForwardCoreThreads = config.getInt(ROLL_FORWARD_LOAD_CORE_THREADS,DEFAULT_ROLL_FORWARD_LOAD_CORE_THREADS);
        maxConcurrentRollForwards = config.getInt(ROLL_FORWARD_MAX_CONCURRENT_ROLLS,DEFAULT_ROLL_FORWARD_MAX_CONCURENT_ROLLS);
        maxImportProcessingThreads = config.getInt(IMPORT_MAX_PROCESSING_THREADS,DEFAULT_IMPORT_MAX_PROCESSING_THREADS);
        maxImportReadBufferSize = config.getInt(IMPORT_MAX_READ_BUFFER_SIZE,DEFAULT_IMPORT_MAX_READ_BUFFER_SIZE);
	}
	
	public static void reloadConfiguration(Configuration configuration) {
		HBaseConfiguration.merge(config,configuration);
		setParameters();
	}
	
}
