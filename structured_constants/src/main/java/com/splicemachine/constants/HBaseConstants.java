package com.splicemachine.constants;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * Class for constants pertaining to HBase.
 * 
 * @author John Leach
 * @version %I%, %G%
 */
public class HBaseConstants {
	public static final String DEFAULT_FAMILY = "attributes";
	public static final byte[] DEFAULT_FAMILY_BYTES = Bytes.toBytes("attributes");
	public static final int DEFAULT_VERSIONS = HColumnDescriptor.DEFAULT_VERSIONS;
	public static final String DEFAULT_COMPRESSION = "none";
	public static final Boolean DEFAULT_IN_MEMORY = HColumnDescriptor.DEFAULT_IN_MEMORY;
	public static final Boolean DEFAULT_BLOCKCACHE=HColumnDescriptor.DEFAULT_BLOCKCACHE;
	public static final int DEFAULT_TTL = HColumnDescriptor.DEFAULT_TTL;
	public static final int DEFAULT_POOLTIME_EVICTION_RUNS = 15 * 1000;
	public static final int DEFAULT_POOL_EVICTABLE_ITEM_TIME = 30 * 1000;
	public static final String DEFAULT_RELATIONSHIP_INDEX_PREFIX = "RELATIONSHIP-";
	public static final String DEFAULT_DSI_FIELD_PREFIX = "DSI-";
	public static final String DEFAULT_BLOOMFILTER = HColumnDescriptor.DEFAULT_BLOOMFILTER;
	
	public static final String VENDOR_HBASE = "HBase";
	public static final String KEY_FAMILY = "family";
	public static final String FAMILY_SEPARATOR=":";
	public static final String SPACE = " ";
//	public static final String TABLE_COMPRESSION = "com.splicemachine.table.compression";
	public static final String HBASE_ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
	public static final String HBASE_ZOOKEEPER_QUOROM = "hbase.zookeeper.quorum";
	public static final byte[] TABLE_CONFIGURATION = "TABLE_CONFIGURATION".getBytes();
	public static final int HTABLE_POOL_SIZE = 10;
	public static final int SHEMACREATION_TIMEOUT = 30;
	
	/**
	 * Persistence properties
	 */
	public static final String PERSISTENCE_PARALLELIZE_SCHEMA_CREATION = "com.ir.store.hbase.parallelizeSchemaCreation";
	
}
