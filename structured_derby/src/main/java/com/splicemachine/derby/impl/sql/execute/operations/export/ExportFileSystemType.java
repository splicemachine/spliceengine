package com.splicemachine.derby.impl.sql.execute.operations.export;

/**
 * Represents the possible destination file systems for our distributed export operation.
 */
public enum ExportFileSystemType {

    // Currently the only option is HDFS.
    HDFS;

    public static boolean isValid(String value) {
        for (ExportFileSystemType t : values()) {
            if (t.name().toUpperCase().equals(value)) {
                return true;
            }
        }
        return false;
    }

}
