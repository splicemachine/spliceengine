package com.splicemachine.db.iapi.types;

import java.io.*;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 *
 * Array Implementation for over the wire communication...
 *
 */
public class ArrayImpl implements Array, Serializable, Externalizable {

    @Override
    public String getBaseTypeName() throws SQLException {
        return null;
    }

    @Override
    public int getBaseType() throws SQLException {
        return 0;
    }

    @Override
    public Object getArray() throws SQLException {
        return null;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return null;
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public void free() throws SQLException {

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }
}
