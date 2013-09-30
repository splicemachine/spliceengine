package com.splicemachine.derby.utils.test;

import com.google.common.base.Charsets;
import com.splicemachine.encoding.MultiFieldDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.types.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/**
 * @author Scott Fines
 *         Created on: 9/30/13
 */
public enum ImportDataType {
    BOOLEAN(Types.BOOLEAN){
        @Override
        public DataValueDescriptor getDataValueDescriptor() {
            return new SQLBoolean();
        }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextBoolean());
        }

        @Override
        public String toString(Object value) {
            return Boolean.toString((Boolean)value);
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            boolean val = (Boolean)value;
            dvd.setValue(val);
        }

        @Override
        public Object newObject(Random random) {
            return random.nextBoolean();
        }
    },
    TINYINT(Types.TINYINT){
        @Override
        public DataValueDescriptor getDataValueDescriptor() {
            return new SQLTinyint();
        }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextByte());
        }

        @Override
        public String toString(Object value) {
            return Byte.toString((Byte)value);
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            byte val = (Byte)value;
            dvd.setValue(val);
        }

        @Override
        public Object newObject(Random random) {
            return (byte)random.nextInt();
        }

        @Override
        public boolean isScalarType() {
            return true;
        }
    },
    SMALLINT(Types.SMALLINT){
        @Override
        public DataValueDescriptor getDataValueDescriptor() {
            return new SQLSmallint();
        }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextShort());
        }

        @Override
        public String toString(Object value) {
            return Short.toString((Short)value);
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            short val = (Short)value;
            dvd.setValue(val);
        }

        @Override
        public Object newObject(Random random) {
            return (short)random.nextInt();
        }

        @Override
        public boolean isScalarType() {
            return true;
        }
    },
    INTEGER(Types.INTEGER){
        @Override
        public DataValueDescriptor getDataValueDescriptor() {
            return new SQLInteger();
        }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextInt());
        }

        @Override
        public String toString(Object value) {
            return Integer.toString((Integer)value);
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            int val = (Integer)value;
            dvd.setValue(val);
        }

        @Override
        public Object newObject(Random random) {
            return random.nextInt();
        }

        @Override
        public boolean isScalarType() {
            return true;
        }
    },
    BIGINT(Types.BIGINT){
        @Override
        public DataValueDescriptor getDataValueDescriptor() {
            return new SQLLongint();
        }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextLong());
        }

        @Override
        public String toString(Object value) {
            return Long.toString((Long)value);
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            long val = (Long)value;
            dvd.setValue(val);
        }

        @Override
        public Object newObject(Random random) {
            return random.nextLong();
        }

        @Override
        public boolean isScalarType() {
            return true;
        }
    },
    REAL(Types.REAL){
        @Override
        public DataValueDescriptor getDataValueDescriptor() {
            return new SQLReal();
        }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNullFloat())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextFloat());
        }

        @Override
        public String toString(Object value) {
            return Float.toString((Float)value);
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            float theValue = (Float) value;
            dvd.setValue(theValue);
        }

        @Override
        public Object newObject(Random random) {
            return random.nextFloat();
        }
    },
    DOUBLE(Types.DOUBLE){
        @Override
        public DataValueDescriptor getDataValueDescriptor() {
            return new SQLDouble();
        }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNullDouble())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextDouble());
        }

        @Override
        public String toString(Object value) {
            return Double.toString((Double)value);
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            double theValue = (Double) value;
            dvd.setValue(theValue);
        }

        @Override
        public Object newObject(Random random) {
            return random.nextDouble();
        }
    },
    DECIMAL(Types.DECIMAL){
        @Override
        public DataValueDescriptor getDataValueDescriptor() {
            return new SQLDecimal();
        }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setBigDecimal(decoder.decodeNextBigDecimal());
        }

        @Override
        public String toString(Object value) {
            return value.toString();
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            dvd.setBigDecimal((BigDecimal) value);
        }

        @Override
        public Object newObject(Random random) {
            return new BigDecimal(new BigInteger(100,random));
        }
    },
    VARCHAR(Types.VARCHAR){
        @Override
        public DataValueDescriptor getDataValueDescriptor() {
            return new SQLVarchar();
        }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextString());
        }

        @Override
        public String toString(Object value) {
            return (String)value;
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            dvd.setValue((String)value);
        }

        @Override
        public Object newObject(Random random) {
            char[] string = new char[random.nextInt(100)];
            Charset charset = Charsets.UTF_8;
            CharsetEncoder encoder = charset.newEncoder().onMalformedInput(CodingErrorAction.REPORT);
            for(int i=0;i<string.length;i++){
                char next = (char)random.nextInt();
                while(!encoder.canEncode(next))
                    next = (char)random.nextInt();

                string[i] = next;
            }
            return new String(string);
        }
    },
    CHAR(Types.CHAR){
        @Override
        public DataValueDescriptor getDataValueDescriptor() {
            return new SQLChar();
        }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextString());
        }

        @Override
        public String toString(Object value) {
            return (String)value;
        }
        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            dvd.setValue((String)value);
        }

        @Override
        public Object newObject(Random random) {
            char[] string = new char[random.nextInt(100)];
            Charset charset = Charsets.UTF_8;
            CharsetEncoder encoder = charset.newEncoder().onMalformedInput(CodingErrorAction.REPORT);
            for(int i=0;i<string.length;i++){
                char next = (char)random.nextInt();
                while(!encoder.canEncode(next))
                    next = (char)random.nextInt();

                string[i] = next;
            }
            return new String(string);
        }
    },
    DATE(Types.DATE){
        @Override
        public DataValueDescriptor getDataValueDescriptor() {
            return new SQLDate();
        }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setValue(new java.sql.Timestamp(decoder.decodeNextLong()));
        }

        @Override
        public String toString(Object value) {
            return new SimpleDateFormat(getDateFormat()).format(new Date((Long)value));
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            long next = Math.abs((Long) value);
            dvd.setValue(new java.sql.Date(next));
        }

        @Override
        public String getDateFormat() {
            return "yyyy-MM-dd";
        }

        @Override
        public Object newObject(Random random) {
            return new Date().getTime();
        }
    },
    TIME(Types.TIME){
        @Override
        public DataValueDescriptor getDataValueDescriptor() {
            return new SQLTime();
        }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setValue(new java.sql.Time(decoder.decodeNextLong()));
        }

        @Override
        public String toString(Object value) {
            return new SimpleDateFormat(getTimeFormat()).format(new Time((Long)value));
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            long next = Math.abs((Long)value);
            dvd.setValue(new java.sql.Time(next));
        }

        @Override
        public Object newObject(Random random) {
            return new Date().getTime();
        }

        @Override
        public String getTimeFormat() {
            return "HH:mm:ss";
        }

    },
    TIMESTAMP(Types.TIMESTAMP){
        @Override
        public DataValueDescriptor getDataValueDescriptor() {
            return new SQLTimestamp();
        }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setValue(new java.sql.Timestamp(decoder.decodeNextLong()));
        }

        @Override
        public String toString(Object value) {
            return new SimpleDateFormat(getTimestampFormat()).format(new Timestamp((Long)value));
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            long next = Math.abs((Long) value);
            dvd.setValue(new java.sql.Timestamp(next));
        }

        @Override
        public Object newObject(Random random) {
            return new Timestamp(new Date().getTime()).getTime();
        }

        @Override
        public String getTimestampFormat() {
            return "yyyy-MM-dd HH:mm:ss.SSS";
        }
    };

    private final int jdbcType;
    private String timeFormat;

    private ImportDataType(int jdbcType) {
        this.jdbcType = jdbcType;
    }

    public int getJdbcType(){
        return jdbcType;
    }

    public DataValueDescriptor getDataValueDescriptor(){
        throw new UnsupportedOperationException();
    }

    public void decodeNext(DataValueDescriptor dvd,MultiFieldDecoder decoder) throws StandardException{
        throw new UnsupportedOperationException();
    }

    public String toString(Object value){
        throw new UnsupportedOperationException();
    }

    public void setNext(DataValueDescriptor dvd, Object value) throws StandardException{
        throw new UnsupportedOperationException();
    }


    public String getTimestampFormat() {
        return null;
    }

    public String getDateFormat() {
        return null;
    }

    public String getTimeFormat() {
        return null;
    }

    public Object newObject(Random random) {
        return null;
    }

    public boolean isScalarType() {
        return false;
    }
}
