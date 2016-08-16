/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.cluster;


import com.splicemachine.db.client.am.*;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.shared.common.reference.JDBC40Translation;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.RowId;
import java.sql.Types;
import java.util.Arrays;
import java.util.Calendar;

/**
 * Handles parameters for a prepared statement.
 *
 * Technically, our Clustering logic could defer this to a delegate prepared statement (since derby handles it).
 * Unfortunately, we don't necessarily push statement parameters into the delegated statement right away (if, for example,
 * the underlying delegate hasn't been constructed yet), so we wouldn't necessarily get the correct error message, or
 * have the error message thrown at the correct time. Therefore, we have to do all parameter verification outside
 * of the delegate statement.
 *
 * Hence, this class performs that basic logic. We pulled most of the parameter verification from Derby's client
 * PreparedStatement class, modernized it, and stuck it in here.
 *
 * @author Scott Fines
 *         Date: 9/21/16
 */
public class StatementParameters{

    private Object[] parameterValues;
    private boolean[] parameterSet;
    private int[] types;
    private ResultSetMetaData parameterMetaData;
    /*
     * Some data types are only traversable once (i.e. InputStreams and Readers). This means that we can't
     * automatically retry any prepared statement which has one of those as a parameter. This flag keeps track
     * of that
     */
    private boolean retryable = true;


    public ResultSetMetaData metadata(){
        return parameterMetaData;
    }

    void reset() {
        Arrays.fill(parameterSet,false);
        Arrays.fill(parameterValues,null);
        Arrays.fill(types,-1);
        retryable = true;
    }

    StatementParameters(ResultSetMetaData parameterMetaData) throws SQLException{
        this.parameterMetaData=parameterMetaData;
        int pCount = parameterMetaData.getColumnCount();
        this.parameterValues = new Object[pCount];
        this.parameterSet = new boolean[pCount];
        this.types = new int[pCount];

    }

    StatementParameters(StatementParameters copy){
        this.parameterMetaData = copy.parameterMetaData;
        this.parameterValues = Arrays.copyOf(copy.parameterValues,copy.parameterValues.length);
        this.parameterSet = Arrays.copyOf(copy.parameterSet,copy.parameterSet.length);
        this.types = Arrays.copyOf(copy.types,copy.types.length);
        this.retryable = copy.retryable;
    }

    boolean canRetry(){
        return retryable;
    }

    void setNull(int parameterIndex, int jdbcType) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.getPossibleTypesForNull(paramType).performTypeCheck(paramType,jdbcType);
        if(parameterMetaData.isNullable(parameterIndex)==ResultSetMetaData.columnNoNulls){
            throw new SqlException(null,new ClientMessageId(SQLState.LANG_NULL_INTO_NON_NULL),parameterIndex).getSQLException();
        }
        types[parameterIndex-1] = jdbcType;

        setValue(parameterIndex,null);
    }


    void setNull(int parameterIndex,int jdbcType,String typeName) throws SQLException{
        setNull(parameterIndex,jdbcType);
    }

    void setBoolean(int parameterIndex, boolean x) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.performTypeCheck(paramType,Types.BOOLEAN);
        types[parameterIndex-1] = Types.BOOLEAN;
        setValue(parameterIndex,x);
    }

    void setByte(int parameterIndex, byte x) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.performTypeCheck(paramType,Types.TINYINT);
        types[parameterIndex-1] = Types.TINYINT;
        setValue(parameterIndex,x);
    }

    void setShort(int parameterIndex,short x) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.performTypeCheck(paramType,Types.SMALLINT);
        types[parameterIndex-1] = Types.SMALLINT;
        setValue(parameterIndex,x);
    }

    void setInt(int parameterIndex, int x) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.performTypeCheck(paramType,Types.INTEGER);
        types[parameterIndex-1] = Types.INTEGER;
        setValue(parameterIndex,x);
    }

    void setLong(int parameterIndex, long x) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.performTypeCheck(paramType,Types.BIGINT);
        types[parameterIndex-1] = Types.BIGINT;
        setValue(parameterIndex,x);
    }

    void setFloat(int parameterIndex, float x) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.performTypeCheck(paramType,Types.FLOAT);
        types[parameterIndex-1] = Types.REAL;
        setValue(parameterIndex,x);
    }

    void setDouble(int parameterIndex, double x) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.performTypeCheck(paramType,Types.DOUBLE);
        types[parameterIndex-1] = Types.DOUBLE;
        setValue(parameterIndex,x);
    }

    void setBigDecimal(int parameterIndex,BigDecimal x) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_GENERIC_SCALAR.performTypeCheck(paramType,Types.DECIMAL);
        types[parameterIndex-1] = Types.DECIMAL;
        setValue(parameterIndex,x);
    }

    void setDate(int parameterIndex,Date x,Calendar calendar) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_DATE.performTypeCheck(paramType,Types.DATE);
        if(calendar==null)
            throw new SqlException(null,new ClientMessageId(SQLState.INVALID_API_PARAMETER),"null","calendar","setDate()").getSQLException();

        types[parameterIndex-1] = Types.DATE;
        if(x==null)
            setNull(parameterIndex,Types.DATE);
        else
            setValue(parameterIndex,new DateTimePair(x,calendar));
    }

    void setDate(int parameterIndex,Date x) throws SQLException{
        setDate(parameterIndex, x,Calendar.getInstance());

    }

    void setTime(int parameterIndex,Time x,Calendar calendar) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_TIME.performTypeCheck(paramType,Types.TIME);
        if(calendar==null)
            throw new SqlException(null,new ClientMessageId(SQLState.INVALID_API_PARAMETER),"null","calendar","setTime()").getSQLException();

        types[parameterIndex-1] = Types.TIME;
        if(x==null)
            setNull(parameterIndex,Types.TIME);
        else
            setValue(parameterIndex,new DateTimePair(x,calendar));
    }

    void setTime(int parameterIndex,Time x) throws SQLException{
        setTime(parameterIndex, x,Calendar.getInstance());
    }

    void setTimestamp(int parameterIndex,Timestamp x,Calendar calendar) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_TIMESTAMP.performTypeCheck(paramType,Types.TIMESTAMP);
        if(calendar==null)
            throw new SqlException(null,new ClientMessageId(SQLState.INVALID_API_PARAMETER),"null","calendar","setTimestamp()").getSQLException();

        types[parameterIndex-1] = Types.TIMESTAMP;
        if(x==null)
            setNull(parameterIndex,Types.TIMESTAMP);
        else
            setValue(parameterIndex,new DateTimePair(x,calendar));

    }

    void setTimestamp(int parameterIndex,Timestamp x) throws SQLException{
        setTimestamp(parameterIndex, x,Calendar.getInstance());

    }

    void setString(int parameterIndex, String x) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_STRING.performTypeCheck(paramType,Types.VARCHAR);
        types[parameterIndex-1] = Types.VARCHAR;
        setValue(parameterIndex,x);
    }

    void setBytes(int parameterIndex, byte[] x) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_BYTES.performTypeCheck(paramType,Types.VARBINARY);
        types[parameterIndex-1] = Types.VARBINARY;
        setValue(parameterIndex,x);
    }

    void setRowId(int parameterIndex, RowId x) throws SQLException{
        types[parameterIndex-1] = Types.ROWID;
        setValue(parameterIndex,x);
    }

    void setBinaryStream(int parameterIndex,InputStream is,long length) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_BINARYSTREAM.performTypeCheck(paramType,Types.LONGVARBINARY);
        types[parameterIndex-1]=Types.LONGVARBINARY;
        if(is==null)
            setNull(parameterIndex,Types.VARBINARY);
        else{
            retryable=false;
            setValue(parameterIndex,new LengthValue(is,LengthValueType.BINARY,length));
        }
    }

    void setAsciiStream(int parameterIndex,InputStream is,long length) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_ASCIISTREAM.performTypeCheck(paramType,Types.LONGVARCHAR);
        types[parameterIndex-1]=Types.LONGVARCHAR;
        if(is==null)
            setNull(parameterIndex,Types.LONGVARCHAR);
        else{
            retryable=false;
            setValue(parameterIndex,new LengthValue(is,LengthValueType.ASCII,length));
        }
    }

    void setUnicodeStream(int parameterIndex,InputStream is,long length) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_ASCIISTREAM.performTypeCheck(paramType,Types.LONGVARCHAR);
        types[parameterIndex-1]=Types.LONGVARCHAR;
        if(is==null)
            setNull(parameterIndex,Types.LONGVARCHAR);
        else{
            retryable=false;
            setValue(parameterIndex,new LengthValue(is,LengthValueType.UNICODE,length));
        }
    }

    void setCharacterStream(int parameterIndex,Reader x) throws SQLException{
        setCharacterStream(parameterIndex, x,-1L);
    }

    void setCharacterStream(int parameterIndex,Reader x, long length) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_ASCIISTREAM.performTypeCheck(paramType,Types.LONGVARCHAR);
        types[parameterIndex-1]=Types.LONGVARCHAR;
        if(x==null)
            setNull(parameterIndex,Types.LONGVARCHAR);
        else{
            retryable=false;
            setValue(parameterIndex,new LengthValue(x,LengthValueType.CHARACTER,length));
        }
    }

    void setBlob(int parameterIndex,Blob x) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_BLOB.performTypeCheck(paramType,Types.BLOB);
        types[parameterIndex-1]=Types.BLOB;
        if(x==null)
            setNull(parameterIndex,Types.BLOB);
        else{
            retryable=false;
            setValue(parameterIndex,new LengthValue(x,LengthValueType.BINARY,-1L));
        }
    }

    void setBlob(int parameterIndex,InputStream x) throws SQLException{
        setBlob(parameterIndex,x,-2L);
    }

    void setBlob(int parameterIndex,InputStream x,long length) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_BLOB.performTypeCheck(paramType,Types.BLOB);
        types[parameterIndex-1]=Types.BLOB;
        if(x==null)
            setNull(parameterIndex,Types.BLOB);
        else{
            retryable=false;
            setValue(parameterIndex,new LengthValue(x,LengthValueType.BINARY,length));
        }
    }

    void setClob(int parameterIndex,Reader x) throws SQLException{
        setClob(parameterIndex,x,-2L);
    }

    void setClob(int parameterIndex,Clob x) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_CLOB.performTypeCheck(paramType,Types.CLOB);
        types[parameterIndex-1]=Types.CLOB;
        if(x==null)
            setNull(parameterIndex,Types.CLOB);
        else {
            retryable=false;
            setValue(parameterIndex,new LengthValue(x,LengthValueType.CHARACTER,-1L));
        }
    }

    void setClob(int parameterIndex,Reader x,long length) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        PossibleTypes.POSSIBLE_TYPES_IN_SET_CLOB.performTypeCheck(paramType,Types.CLOB);
        types[parameterIndex-1]=Types.CLOB;
        if(x==null)
            setNull(parameterIndex,Types.CLOB);
        else {
            retryable=false;
            setValue(parameterIndex,new LengthValue(x,LengthValueType.CHARACTER,length));
        }
    }

    void setObject(int parameterIndex,Object x) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        if(paramType==Types.JAVA_OBJECT)
            setUDT(parameterIndex,x);
        else if(x==null)
            setNull(parameterIndex,paramType);
        else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        } else if (x instanceof Integer) {
            setInt(parameterIndex, ((Integer) x));
        } else if (x instanceof Double) {
            setDouble(parameterIndex, ((Double) x));
        } else if (x instanceof Float) {
            setFloat(parameterIndex, ((Float) x));
        } else if (x instanceof Boolean) {
            setBoolean(parameterIndex, ((Boolean) x));
        } else if (x instanceof Long) {
            setLong(parameterIndex, ((Long) x));
        } else if (x instanceof byte[]) {
            setBytes(parameterIndex, (byte[]) x);
        } else if (x instanceof java.math.BigDecimal) {
            setBigDecimal(parameterIndex, (java.math.BigDecimal) x);
        } else if (x instanceof java.sql.Date) {
            setDate(parameterIndex, (java.sql.Date) x);
        } else if (x instanceof java.sql.Time) {
            setTime(parameterIndex, (java.sql.Time) x);
        } else if (x instanceof java.sql.Timestamp) {
            setTimestamp(parameterIndex, (java.sql.Timestamp) x);
        } else if (x instanceof java.sql.Blob) {
            setBlob(parameterIndex, (java.sql.Blob) x);
        } else if (x instanceof java.sql.Clob) {
            setClob(parameterIndex, (java.sql.Clob) x);
        } else if (x instanceof Short) {
            setShort(parameterIndex,(Short)x);
        } else if (x instanceof java.math.BigInteger) {
            setBigDecimal(parameterIndex, new java.math.BigDecimal( (java.math.BigInteger) x ) );
        } else if (x instanceof java.util.Date) {
            setTimestamp(parameterIndex, new Timestamp(  ((java.util.Date) x).getTime() ) );
        } else if (x instanceof java.util.Calendar) {
            setTimestamp(parameterIndex, new Timestamp(  ((java.util.Calendar) x).getTime().getTime() ) );
        } else if (x instanceof Byte) {
            setByte(parameterIndex,(Byte)x);
        } else if (x instanceof com.splicemachine.db.client.am.RowId) {
            setRowId(parameterIndex, (java.sql.RowId) x);
        } else {
            throw new SqlException(null,
                    new ClientMessageId(com.splicemachine.db.shared.common.reference.SQLState.UNSUPPORTED_TYPE)).getSQLException();
        }
    }

    void setObject(int parameterIndex,Object x,int targetJdbcType) throws SQLException{
        setObject(parameterIndex, x, targetJdbcType,0);
    }

    void setObject(int parameterIndex,Object x,int targetJdbcType,int scale) throws SQLException{
       if(x==null){
           setNull(parameterIndex,targetJdbcType);
           return;
       }

        types[parameterIndex-1] = targetJdbcType;
        setValue(parameterIndex,new LengthValue(x,LengthValueType.OBJECT,scale));
    }

    void setUDT(int parameterIndex,Object x) throws SQLException{
        int paramType = parameterMetaData.getColumnType(parameterIndex);
        if(paramType!=Types.JAVA_OBJECT)
            PossibleTypes.throwtypeMismatch(Types.JAVA_OBJECT,paramType);
        types[parameterIndex-1] = Types.JAVA_OBJECT;
        if(x==null)
            setNull(parameterIndex,Types.JAVA_OBJECT);
        else
            setValue(parameterIndex,x);
    }

    void setParameters(PreparedStatement ps) throws SQLException{
        for(int i=0;i<parameterValues.length;i++){
            if(!parameterSet[i]) continue;
            Object o = parameterValues[i];
            if(o==null){
                ps.setNull(i+1,types[i]);
                continue;
            }

            switch(types[i]){
                case Types.DATE:
                    DateTimePair dtp = (DateTimePair)o;
                    ps.setDate(i+1,(Date)dtp.o,dtp.c);
                    break;
                case Types.TIME:
                    dtp = (DateTimePair)o;
                    ps.setTime(i+1,(Time)dtp.o,dtp.c);
                    break;
                case Types.TIMESTAMP:
                    dtp = (DateTimePair)o;
                    ps.setTimestamp(i+1,(Timestamp)dtp.o,dtp.c);
                    break;
                case Types.LONGVARBINARY:
                    LengthValue lv = (LengthValue)o;
                    if(lv.length<0)
                        ps.setBinaryStream(i+1,(InputStream)lv.is);
                    else
                        ps.setBinaryStream(i+1,(InputStream)lv.is,lv.length);
                    break;
                case Types.BLOB:
                    lv = (LengthValue)o;
                    if(lv.length==-2){
                        ps.setBlob(i+1,(InputStream)lv.is);
                    }else if(lv.length==-1){
                        ps.setBlob(i+1,(Blob)lv.is);
                    }else{
                        ps.setBlob(i+1,(InputStream)lv.is,lv.length);
                    }
                case Types.CLOB:
                    lv = (LengthValue)o;
                    if(lv.length==-2){
                        ps.setClob(i+1,(Reader)lv.is);
                    }else if(lv.length==-1){
                        ps.setClob(i+1,(Clob)lv.is);
                    }else{
                        ps.setClob(i+1,(Reader)lv.is,lv.length);
                    }
                case Types.JAVA_OBJECT:
                    if(o instanceof LengthValue){
                        ps.setObject(i+1,((LengthValue)o).is,(int)((LengthValue)o).length);
                    }else
                        ps.setObject(i+1,o);
                    break;
                case Types.ROWID:
                    ps.setRowId(i+1,(RowId)o);
                    break;
                default:
                    if(o instanceof LengthValue)
                        setStream(ps,i+1,(LengthValue)o);
                    else
                        ps.setObject(i+1,o,types[i]);
            }

        }
    }

    private void setStream(PreparedStatement ps,int i,LengthValue lv) throws SQLException{
        long length = lv.length;
        switch(lv.type){
            case BINARY:
                if(length<0)
                    ps.setBinaryStream(i+1,(InputStream)lv.is);
                else
                    ps.setBinaryStream(i+1,(InputStream)lv.is,length);
                break;
            case ASCII:
                if(length<0)
                    ps.setAsciiStream(i+1,(InputStream)lv.is);
                else
                    ps.setAsciiStream(i+1,(InputStream)lv.is,length);
                break;
            case UNICODE:
                ps.setUnicodeStream(i+1,(InputStream)lv.is,(int)length);
                break;
            case CHARACTER:
                if(length<0)
                    ps.setCharacterStream(i+1,(Reader)lv.is);
                else
                    ps.setCharacterStream(i+1,(Reader)lv.is,length);
                break;
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void setValue(int parameterIndex,Object value){
        parameterValues[parameterIndex-1] = value;
        parameterSet[parameterIndex-1] = true;
    }

    private class DateTimePair{
        private Object o;
        private Calendar c;

        DateTimePair(Object x,Calendar calendar){
            this.o = x;
            this.c = calendar;
        }
    }

    private class LengthValue{
        private final Object is;
        private final long length;
        private final LengthValueType type;

        LengthValue(Object is,LengthValueType type,long length){
            this.is=is;
            this.length=length;
            this.type = type;
        }
    }

    private enum LengthValueType{
        BINARY,
        ASCII,
        UNICODE,
        OBJECT,
        CHARACTER
    }

}
