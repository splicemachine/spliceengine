package com.splicemachine.sql;

/**
 * @author Scott Fines
 *         Date: 1/28/15
 */
public interface ColumnMetaData {
    boolean isAutoIncrement();

    boolean isCaseSensitive();

    boolean isSearchable();

    boolean isCurrency();

    ColumnNullability nullability();

    boolean isSigned();

    int getColumnDisplaySize();

    String getLabel();

    String getName();

    String getSchema();

    String getTable();

    String getCatalog();

    int getPrecision();

    int getScale();

    SQLType getType();

    boolean isReadOnly();

    boolean isWritable();

    String getColumnClassName();
}
