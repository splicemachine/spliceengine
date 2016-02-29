package com.splicemachine.derby.stream.spark;

public class SparkConstants {
    
    //
    // RDD Names for implicitly created RDDs. For names to use for RDDs
    // associated with Functions, see StreamUtils.
    //
    public static final String RDD_NAME_SINGLE_ROW_DATA_SET = "Prepare Single Row Data Set";
    public static final String RDD_NAME_EMPTY_DATA_SET = "Prepare Data Set";
    public static final String RDD_NAME_READ_TEXT_FILE = "Read File";
    public static final String RDD_NAME_SCAN_TABLE = "Scan Table %s";
    public static final String RDD_NAME_GET_VALUES = "Read Values";
    public static final String RDD_NAME_SUBTRACTBYKEY = "Subtract Right From Left";
    public static final String RDD_NAME_UNION = "Perform Union";

    //
    // Strings to use for custom spark scope names.
    //
    
    public static final String SCOPE_NAME_READ_TEXT_FILE = "Read File";
    public static final String SCOPE_NAME_PARSE_FILE = "Parse File";
    public static final String SCOPE_SORT_KEYER = "Prepare Keys";
    public static final String SCOPE_GROUP_AGGREGATE_KEYER = SCOPE_SORT_KEYER;
}
