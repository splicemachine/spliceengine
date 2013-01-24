package com.splicemachine.hbase.txn.logger;

public class LogConstants {
	public static final String ACTION_TYPE = "ACTION_TYPE";
	public static final String ROW_KEY = "ROW_KEY";
	public static final byte[] LOG_NODE_DELIMITER = "$%$__".getBytes();
	public static final String ACTION_WRITABLE = "ACTION_WRITABLE";
	public static final String TXN_ID_COLUMN = "TXN_ID_COLUMN";
	public static final String LOG_DELIMITER = "&%@__";
	public static final byte[] ROW_KEY_BYTES = ROW_KEY.getBytes();
	public static final byte[] TXN_ID_COLUMN_BYTES = TXN_ID_COLUMN.getBytes();
	public static final byte[] LOG_DELIMITER_BYTES = LOG_DELIMITER.getBytes();
	public static final byte[] ACTION_WRITABLE_BYTE = ACTION_WRITABLE.getBytes();
	public static final byte[] ACTION_TYPE_BYTES = ACTION_TYPE.getBytes();
	public static final byte[] SPLIT_GENERATED_TOKEN_BYTES = "SPLIT_GENERATED_TOKEN_BYTES__".getBytes();
	public static final String LOG_PATH_NAME = "hbase.log.path";
	public static final String DEFAULT_LOG_PATH = "/DEFAULT_LOG_PATH";
	public static final String TXN_LOG_SUBPATH = "/txnLog";
	public static final String SPLIT_LOG_SUBPATH = "/splitLog";
	public static final String REGION_LOG_SUBPATH = "/regionLog";
	public static final String SPLIT_LOG_REGION_POSTFIX = "/splitRegion-";
	
	public enum WriteActionType {
		PUT,
		DELETE
	}
	/*public enum LogStatus {
		TXN_LOG,
		SPLIT_LOG,
		NO_LOG
	}*/
	public enum LogRecordType {
		TXN_LOG,
		SPLIT_LOG,
	}
}
