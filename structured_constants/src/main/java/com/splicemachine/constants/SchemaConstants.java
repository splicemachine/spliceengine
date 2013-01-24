package com.splicemachine.constants;

import org.apache.hadoop.hbase.util.Bytes;

public class SchemaConstants {
	public static final String VENDOR_HBASE = "HBase";
	public static final String KEY_FAMILY = "family";
	public static final String SPACE = " ";
	public final static String PATH_DELIMITER = "/";
	public static final String TABLE_STRUCTURE = "TABLE_STRUCTURE";
	public static final String INDEXTABLE_STRUCTURE = "INDEXTABLE_STRUCTURE";
	public static final String INDEX = "__INDEX";
	public static final String COLUMN = "__COLUMN";
	public static final String SERIALIZED_INDEX = "SERIALIZED_INDEX";
	public static final String SERIALIZED_COLUMN = "SERIALIZED_COLUMN";
	public static final byte[] INDEX_BASE_FAMILY_BYTE = Bytes.toBytes("INDEX_BASE_FAMILY_BYTE");
	public static final byte[] INDEX_BASE_ROW_BYTE = Bytes.toBytes("INDEX_BASE_ROW");
	public static final String SCHEMA_PATH_NAME = "hbase.schema.path";
	public static final String DEFAULT_SCHEMA_PATH = "/DEFAULT_SCHEMA_PATH";
	public static final byte[] ADD_INDEX_BYTE = Bytes.toBytes("ADD_INDEX_BYTE");
	public static final byte[] DELETE_INDEX_BYTE = Bytes.toBytes("DELETE_INDEX_BYTE");
	public static final byte[] ADD_COLUMN_BYTE = Bytes.toBytes("ADD_COLUMN_BYTE");
	public static final byte[] DELETE_COLUMN_BYTE = Bytes.toBytes("DELETE_COLUMN_BYTE");
	public static final byte[] INDEX_DELIMITER = Bytes.toBytes("__;&;");
	public static final int FIELD_FLAGS_SIZE = 1;
    public static final byte[] EOF_MARKER = new byte[] {0, 0, 0, 0};
	public static final String CONGLOMERATE_PATH_NAME = "hbase.schema.path";
	public static final String DEFAULT_CONGLOMERATE_SCHEMA_PATH = "/conglomerates";
}
