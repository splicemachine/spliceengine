package com.splicemachine.sql;

import java.sql.ResultSetMetaData;

/**
 * @author Scott Fines
 *         Date: 1/28/15
 */
public enum ColumnNullability {
    NO_NULLS(ResultSetMetaData.columnNoNulls),
    NULLABLE(ResultSetMetaData.columnNoNulls),
    UNKNOWN(ResultSetMetaData.columnNullableUnknown);

    private final int code;

    private ColumnNullability(int code) {
        this.code = code;
    }

    public int code(){
        return code;
    }

}
