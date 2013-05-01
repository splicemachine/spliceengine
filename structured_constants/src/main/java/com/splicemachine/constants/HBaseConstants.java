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
	public static final String DEFAULT_FAMILY = "T";
	public static final byte[] DEFAULT_FAMILY_BYTES = Bytes.toBytes(DEFAULT_FAMILY);
    public static final String TEMP_TABLE = "SYS_TEMP";
    public static byte[] TEMP_TABLE_BYTES = Bytes.toBytes(TEMP_TABLE);
	public static final int DEFAULT_VERSIONS = HColumnDescriptor.DEFAULT_VERSIONS;
	public static final String DEFAULT_COMPRESSION = "none";
	public static final Boolean DEFAULT_IN_MEMORY = HColumnDescriptor.DEFAULT_IN_MEMORY;
	public static final Boolean DEFAULT_BLOCKCACHE=HColumnDescriptor.DEFAULT_BLOCKCACHE;
	public static final int DEFAULT_TTL = HColumnDescriptor.DEFAULT_TTL;
	public static final String DEFAULT_BLOOMFILTER = HColumnDescriptor.DEFAULT_BLOOMFILTER;
	public static final String TABLE_COMPRESSION = "com.splicemachine.table.compression";
	public static final String HBASE_ZOOKEEPER_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
	public static final String HBASE_ZOOKEEPER_QUOROM = "hbase.zookeeper.quorum";
}
