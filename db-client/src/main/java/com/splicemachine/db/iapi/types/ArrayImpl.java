package com.splicemachine.db.iapi.types;

import java.io.*;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 * Array Implementation for over the wire communication...
 *
 */
public class ArrayImpl implements Array, Serializable, Externalizable {
    protected String baseTypeName;
    protected int baseType;
    protected Object[] array;

    /* Required for Serde */
    public ArrayImpl() {}
    public ArrayImpl(String baseTypeName, int baseType, Object[] array) {
        this.baseType = baseType;
        this.baseTypeName = baseTypeName;
        this.array = array;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return baseTypeName;
    }

    @Override
    public int getBaseType() throws SQLException {
        return baseType;
    }

    @Override
    public Object getArray() throws SQLException {
        return array;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return Arrays.copyOfRange(array,(int)index,count,Object[].class);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public void free() throws SQLException {

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(baseTypeName);
        out.writeInt(baseType);
        out.writeObject(array);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        baseTypeName = in.readUTF();
        baseType = in.readInt();
        array = (Object[]) in.readObject();
    }
}
