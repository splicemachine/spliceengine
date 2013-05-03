package com.splicemachine.constants;

import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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
//	public static final String DEFAULT_DERBY_PROPERTY_PATH = "/derbyPropertyPath";
	public static final String DEFAULT_QUERY_NODE_PATH = "/queryNodePath";
	public static final String DEFAULT_STARTUP_PATH = "/startupPath";	
	// Constants
	public static final String DEFAULT_DERBY_BIND_ADDRESS = "0.0.0.0";	
	public static final int DEFAULT_DERBY_BIND_PORT = 1527;	
	public static final int DEFAULT_OPERATION_PRIORITY = 3;
	public static final int DEFAULT_IMPORT_TASK_PRIORITY = 3;
    public static final String NA_TRANSACTION_ID = "NA_TRANSACTION_ID";
    public static final String SI_EXEMPT = "si-exempt";

	
	// Zookeeper Path Configuration Constants
	public static final String CONFIG_BASE_TASK_QUEUE_NODE = "splice.task_queue_node";
    public static final String CONFIG_BASE_JOB_QUEUE_NODE = "splice.job_queue_node";
    public static final String CONFIG_TRANSACTION_PATH = "splice.transactions_node";
	public static final String CONFIG_CONGLOMERATE_SCHEMA_PATH = "splice.conglomerates_node";
//	public static final String CONFIG_DERBY_PROPERTY_PATH = "splice.derby_property_node";
	public static final String CONFIG_QUERY_NODE_PATH = "splice.query_node_path";
	public static final String CONFIG_STARTUP_PATH = "splice.startup_path";
	public static final String CONFIG_DERBY_BIND_ADDRESS = "splice.server.address";
	public static final String CONFIG_DERBY_BIND_PORT = "splice.server.port";
	public static final String CONFIG_OPERATION_PRIORITY = "splice.task.operationPriority";
	public static final String CONFIG_IMPORT_TASK_PRIORITY = "splice.task.importTaskPriority";
	
	// Zookeeper Actual Paths
	public static final String zkSpliceTaskPath;
	public static final String zkSpliceJobPath;
	public static final String zkSpliceTransactionPath;
	public static final String zkSpliceConglomeratePath;
	public static final String zkSpliceConglomerateSequencePath;
//	public static final String zkSpliceDerbyPropertyPath;
	public static final String zkSpliceQueryNodePath;
	public static final String zkSpliceStartupPath;
	public static final String derbyBindAddress;
	public static final int derbyBindPort;
    public static final int operationTaskPriority;
    public static final int importTaskPriority;
	public static final Long sleepSplitInterval;
	
	// Splice Internal Tables
    public static final String TEMP_TABLE = "SPLICE_TEMP";
    public static final String TRANSACTION_TABLE = "SPLICE_TXN";
    public static final String CONGLOMERATE_TABLE_NAME = "SPLICE_CONGLOMERATE";
    public static final String PROPERTIES_TABLE_NAME = "SPLICE_PROPS";
    
    public static byte[] TEMP_TABLE_BYTES = Bytes.toBytes(TEMP_TABLE);
    public static final byte[] TRANSACTION_TABLE_BYTES = Bytes.toBytes(TRANSACTION_TABLE);
    public static final byte[] CONGLOMERATE_TABLE_NAME_BYTES = Bytes.toBytes(CONGLOMERATE_TABLE_NAME);
    public static final byte[]PROPERTIES_TABLE_NAME_BYTES = Bytes.toBytes(PROPERTIES_TABLE_NAME);
    
	// Splice Family Information
	public static final String DEFAULT_FAMILY = "V";
	public static final byte[] DEFAULT_FAMILY_BYTES = Bytes.toBytes(DEFAULT_FAMILY);

	// Splice Default Table Definitions
	public static final int DEFAULT_VERSIONS = HColumnDescriptor.DEFAULT_VERSIONS;
	public static final String DEFAULT_COMPRESSION = "none";
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
    public static final byte[] VALUE_COLUMN = Integer.toString(1).getBytes();
	public static final long DEFAULT_SPLIT_WAIT_INTERVAL = 500l;
	
    // Default Configuration Options
	public static final String SPLIT_WAIT_INTERVAL = "splice.splitWaitInterval";

    
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
		zkSpliceTaskPath = config.get(CONFIG_BASE_TASK_QUEUE_NODE,DEFAULT_BASE_TASK_QUEUE_NODE);
		zkSpliceJobPath = config.get(CONFIG_BASE_JOB_QUEUE_NODE,DEFAULT_BASE_JOB_QUEUE_NODE);
		zkSpliceTransactionPath = config.get(CONFIG_TRANSACTION_PATH,DEFAULT_TRANSACTION_PATH);		
		zkSpliceConglomeratePath = config.get(CONFIG_CONGLOMERATE_SCHEMA_PATH,DEFAULT_CONGLOMERATE_SCHEMA_PATH);
		zkSpliceConglomerateSequencePath = zkSpliceConglomeratePath+"/__CONGLOM_SEQUENCE";
//		zkSpliceDerbyPropertyPath = config.get(CONFIG_DERBY_PROPERTY_PATH,DEFAULT_DERBY_PROPERTY_PATH);
		zkSpliceQueryNodePath = config.get(CONFIG_CONGLOMERATE_SCHEMA_PATH,DEFAULT_CONGLOMERATE_SCHEMA_PATH);
		sleepSplitInterval = config.getLong(SPLIT_WAIT_INTERVAL, DEFAULT_SPLIT_WAIT_INTERVAL);
		zkSpliceStartupPath = config.get(CONFIG_STARTUP_PATH,DEFAULT_STARTUP_PATH);
        derbyBindAddress = config.get(CONFIG_DERBY_BIND_ADDRESS,DEFAULT_DERBY_BIND_ADDRESS);
        derbyBindPort = config.getInt(CONFIG_DERBY_BIND_PORT, DEFAULT_DERBY_BIND_PORT);
        operationTaskPriority = config.getInt(CONFIG_OPERATION_PRIORITY, DEFAULT_OPERATION_PRIORITY);
        importTaskPriority = config.getInt(CONFIG_IMPORT_TASK_PRIORITY, DEFAULT_IMPORT_TASK_PRIORITY);
	}
	
	public static List<String> zookeeperPaths = Lists.newArrayList(zkSpliceTaskPath,zkSpliceJobPath,
			zkSpliceTransactionPath,zkSpliceConglomeratePath,zkSpliceConglomerateSequencePath,zkSpliceQueryNodePath);

	public static List<byte[]> spliceSystables = Lists.newArrayList(TEMP_TABLE_BYTES,TRANSACTION_TABLE_BYTES,CONGLOMERATE_TABLE_NAME_BYTES);
	
	
}
