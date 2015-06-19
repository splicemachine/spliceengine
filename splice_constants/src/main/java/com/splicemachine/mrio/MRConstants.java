package com.splicemachine.mrio;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * 
 * Splice Constants...
 * 
 *
 */
public class MRConstants {
	public final static String SPLICE_TRANSACTION_ID = "transaction.id";
	final public static String SPLICE_TABLE_NAME = "splice.tableName";
	final public static String SPLICE_CONGLOMERATE = "splice.conglomerate";	
	final public static String SPLICE_WRITE_BUFFER_SIZE = "splice.write.buffer.size";
	final public static String SPLICE_JDBC_STR = "splice.jdbc";
	final public static String SPLICE_SCAN_INFO = "splice.scan.info";
	final public static String HBASE_OUTPUT_TABLE_NAME = "hbase_output_tableName";
	final public static String SPLICE_SCAN_MEMSTORE_ONLY="MR";
    final public static String SPLICE_TBLE_CONTEXT="splice.tableContext";
	final public static byte[] FLUSH = Bytes.toBytes("F");
	final public static byte[] HOLD = Bytes.toBytes("H");
	final public static KeyValue MEMSTORE_BEGIN = new KeyValue(HConstants.EMPTY_START_ROW,HOLD,HOLD);
	final public static KeyValue MEMSTORE_END = new KeyValue(HConstants.EMPTY_END_ROW,HOLD,HOLD);	
	final public static KeyValue MEMSTORE_BEGIN_FLUSH = new KeyValue(HConstants.EMPTY_START_ROW,FLUSH,FLUSH);	
	
}