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
    final public static String SPLICE_INPUT_TABLE_NAME="splice.input.tableName";
    final public static String SPLICE_OUTPUT_TABLE_NAME="splice.output.tableName";
    final public static String SPLICE_CONGLOMERATE = "splice.conglomerate";
    final public static String SPLICE_INPUT_CONGLOMERATE = "splice.input.conglomerate";
    final public static String SPLICE_OUTPUT_CONGLOMERATE = "splice.output.conglomerate";
	final public static String SPLICE_WRITE_BUFFER_SIZE = "splice.write.buffer.size";
	final public static String SPLICE_JDBC_STR = "splice.jdbc";
    final public static String DEFAULT_SPLICE_JDBC_STR_VALUE = "jdbc:splice://localhost:1527/splicedb;user=splice;password=admin";
    final public static String ONE_SPLIT_PER_REGION = "one.split.per.region";
	final public static String SPLICE_SCAN_INFO = "splice.scan.info";
	final public static String SPLICE_TXN_MIN_TIMESTAMP = "splice.txn.timestamp.min";
	final public static String SPLICE_TXN_MAX_TIMESTAMP = "splice.txn.timestamp.max";
	final public static String SPLICE_TXN_DEST_TABLE = "splice.txn.destination.table";
	final public static String HBASE_OUTPUT_TABLE_NAME = "hbase_output_tableName";
	final public static String SPLICE_SCAN_MEMSTORE_ONLY="MR";
    final public static String SPLICE_TBLE_CONTEXT="splice.tableContext";
	final public static byte[] FLUSH = Bytes.toBytes("F");
	final public static byte[] HOLD = Bytes.toBytes("H");
	final public static KeyValue MEMSTORE_BEGIN = new KeyValue(HConstants.EMPTY_START_ROW,HOLD,HOLD);
	final public static KeyValue MEMSTORE_END = new KeyValue(HConstants.EMPTY_END_ROW,HOLD,HOLD);	
	final public static KeyValue MEMSTORE_BEGIN_FLUSH = new KeyValue(HConstants.EMPTY_START_ROW,FLUSH,FLUSH);
    final public static String TABLE_WRITER = "table.writer";
    final public static String TABLE_WRITER_TYPE = "table.writer.type";
}