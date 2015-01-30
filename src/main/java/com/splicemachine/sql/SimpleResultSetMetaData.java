package com.splicemachine.sql;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Implementation of a ResultSetMetaData entity.
 *
 * @author Scott Fines
 *         Date: 1/28/15
 */
public abstract class SimpleResultSetMetaData implements ResultSetMetaData{
    private final ColumnMetaData[] columns;

    public SimpleResultSetMetaData(ColumnMetaData[] columns) {
        this.columns = columns;
    }

    @Override public int getColumnCount() throws SQLException { return columns.length; }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].isAutoIncrement();
    }


    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].isCaseSensitive();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].isSearchable();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].isCurrency();
    }

    @Override
    public int isNullable(int column) throws SQLException {
        assertColumnExists(column);
        //noinspection MagicConstant
        return columns[column-1].nullability().code();
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].getColumnDisplaySize();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].getLabel();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].getName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].getSchema();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].getScale();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].getTable();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].getCatalog();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].getType().code();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].getType().name();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].isReadOnly();
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].isWritable();
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return isWritable(column);
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        assertColumnExists(column);
        return columns[column-1].getColumnClassName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Cannot unwrap this Metadata");
    }

    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException { return false; }

    /****************************************************************************************************************/
    /*private helper methods*/
    private void assertColumnExists(int column) throws SQLException {
        if(column<1 || column> columns.length)
            throw new SQLException("Column <"+column+"> does not exist");
    }
}
