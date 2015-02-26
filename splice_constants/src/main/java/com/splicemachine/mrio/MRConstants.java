package com.splicemachine.mrio;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;


/**
 * 
 * Splice Constants...
 * 
 *
 */
public class MRConstants {
	public final static String SPLICE_TRANSACTION_ID = "transaction_id";
	final public static String SPLICE_INPUT_TABLE_NAME = "splice_input_tableName";
	final public static String SPLICE_OUTPUT_TABLE_NAME = "splice_output_tableName";
	final public static String HBASE_OUTPUT_TABLE_NAME = "hbase_output_tableName";
	final public static String SPLICE_WRITE_BUFFER_SIZE = "splice_write_buffer_size";
	final public static String SPLICE_JDBC_STR = "splice_jdbc";
	final public static String SPLICE_SCAN_INFO = "splice_scan_info";
	final public static String SPLICE_SCAN_MEMSTORE_ONLY="MR";
	final public static byte[] FLUSH = "F".getBytes();
	final public static byte[] HOLD = "H".getBytes();
	final public static KeyValue MEMSTORE_BEGIN = new KeyValue(HConstants.EMPTY_START_ROW,HOLD,HOLD);
	final public static KeyValue MEMSTORE_END = new KeyValue(HConstants.EMPTY_END_ROW,HOLD,HOLD);	
	final public static KeyValue MEMSTORE_BEGIN_FLUSH = new KeyValue(HConstants.EMPTY_START_ROW,FLUSH,FLUSH);	
	
}