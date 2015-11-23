package com.splicemachine.derby.stream.spark;

public class SparkConstants {
    
    //
    // RDD Names for implicitly created RDDs. For names to use for RDDs
    // associated with Functions, see StreamUtils.
    //
    public static final String RDD_NAME_SINGLE_ROW_DATA_SET = "Prepare Single Row Data Set";
    public static final String RDD_NAME_READ_TEXT_FILE = "Read CSV File";
    public static final String RDD_NAME_SCAN_TABLE = "Scan Table %s";

    //
    // Strings to use for custom spark scope names.
    //
    
    public static final String SCOPE_NAME_READ_TEXT_FILE = "Read File From Disk";
    public static final String SCOPE_NAME_PARSE_FILE = "Parse File";
}
