/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils.test;

import com.carrotsearch.hppc.BitSet;
import org.spark_project.guava.base.Charsets;
import com.splicemachine.derby.utils.marshall.dvd.TimestampV2DescriptorSerializer;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
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
public enum TestingDataType {
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

        @Override
        public void encode(Object o, MultiFieldEncoder encoder) {
            encoder.encodeNext((Boolean)o);
        }
    },
    TINYINT(Types.TINYINT){
        @Override public DataValueDescriptor getDataValueDescriptor() { return new SQLTinyint(); }
        @Override public String toString(Object value) { return Byte.toString((Byte)value); }
        @Override public Object newObject(Random random) { return (byte)random.nextInt(); }
        @Override public boolean isScalarType() { return true; }
        @Override public void encode(Object o, MultiFieldEncoder encoder) { encoder.encodeNext((Byte)o); }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextByte());
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            byte val = (Byte)value;
            dvd.setValue(val);
        }

    },
    SMALLINT(Types.SMALLINT){
        @Override public void encode(Object o, MultiFieldEncoder encoder) { encoder.encodeNext((Short)o); }
        @Override public DataValueDescriptor getDataValueDescriptor() { return new SQLSmallint(); }
        @Override public String toString(Object value) { return Short.toString((Short)value); }
        @Override public Object newObject(Random random) { return (short)random.nextInt(); }
        @Override public boolean isScalarType() { return true; }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextShort());
        }


        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            short val = (Short)value;
            dvd.setValue(val);
        }

    },
    INTEGER(Types.INTEGER){
        @Override public void encode(Object o, MultiFieldEncoder encoder) { encoder.encodeNext((Integer)o); }
        @Override public DataValueDescriptor getDataValueDescriptor() { return new SQLInteger(); }
        @Override public Object newObject(Random random) { return random.nextInt(); }
        @Override public boolean isScalarType() { return true; }
        @Override public String toString(Object value) { return Integer.toString((Integer)value); }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextInt());
        }


        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            int val = (Integer)value;
            dvd.setValue(val);
        }

    },
    BIGINT(Types.BIGINT){
        @Override public void encode(Object o, MultiFieldEncoder encoder) { encoder.encodeNext((Long)o); }
        @Override public DataValueDescriptor getDataValueDescriptor() { return new SQLLongint(); }
        @Override public String toString(Object value) { return Long.toString((Long)value); }
        @Override public Object newObject(Random random) { return random.nextLong(); }
        @Override public boolean isScalarType() { return true; }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextLong());
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            long val = (Long)value;
            dvd.setValue(val);
        }

    },
    REAL(Types.REAL){
        @Override public void encode(Object o, MultiFieldEncoder encoder) { encoder.encodeNext((Float)o); }
        @Override public DataValueDescriptor getDataValueDescriptor() { return new SQLReal(); }
        @Override public String toString(Object value) { return Float.toString((Float)value); }
        @Override public Object newObject(Random random) { return random.nextFloat(); }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNullFloat())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextFloat());
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            float theValue = (Float) value;
            dvd.setValue(theValue);
        }

    },
    DOUBLE(Types.DOUBLE){
        @Override public DataValueDescriptor getDataValueDescriptor() { return new SQLDouble(); }
        @Override public Object newObject(Random random) { return random.nextDouble(); }
        @Override public void encode(Object o, MultiFieldEncoder encoder) { encoder.encodeNext((Double)o); }
        @Override public String toString(Object value) { return Double.toString((Double)value); }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNullDouble())
                dvd.setToNull();
            else
                dvd.setValue(decoder.decodeNextDouble());
        }

        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            double theValue = (Double) value;
            dvd.setValue(theValue);
        }
    },
    DECIMAL(Types.DECIMAL){
        @Override public void encode(Object o, MultiFieldEncoder encoder) { encoder.encodeNext((BigDecimal)o); }
        @Override public DataValueDescriptor getDataValueDescriptor() { return new SQLDecimal(); }
        @Override public String toString(Object value) { return value.toString(); }
        @Override public Object newObject(Random random) { return new BigDecimal(new BigInteger(100,random)); }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
                dvd.setBigDecimal(decoder.decodeNextBigDecimal());
        }


        @Override
        public void setNext(DataValueDescriptor dvd, Object value) throws StandardException {
            dvd.setBigDecimal((BigDecimal) value);
        }

    },
    VARCHAR(Types.VARCHAR){
        @Override public void encode(Object o, MultiFieldEncoder encoder) { encoder.encodeNext((String)o); }
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
        @Override public void encode(Object o, MultiFieldEncoder encoder) { encoder.encodeNext((String)o); }
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
        @Override public void encode(Object o, MultiFieldEncoder encoder) { encoder.encodeNext((Long)o); }
        @Override public DataValueDescriptor getDataValueDescriptor() { return new SQLDate(); }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else
						  dvd.setValue(new java.sql.Date(decoder.decodeNextLong()));
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

        @Override public boolean isScalarType() { return true; }
    },
    TIME(Types.TIME){
        @Override public void encode(Object o, MultiFieldEncoder encoder) { encoder.encodeNext((Long)o); }
        @Override public boolean isScalarType() { return true; }
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
        @Override public void encode(Object o, MultiFieldEncoder encoder) {
						encoder.encodeNext((Long)o);
				}
        @Override public boolean isScalarType() { return true; }
        @Override
        public DataValueDescriptor getDataValueDescriptor() {
            return new SQLTimestamp();
        }

        @Override
        public void decodeNext(DataValueDescriptor dvd, MultiFieldDecoder decoder) throws StandardException {
            if(decoder.nextIsNull())
                dvd.setToNull();
            else{
								Timestamp timestamp = TimestampV2DescriptorSerializer.parseTimestamp(decoder.decodeNextLong());
								dvd.setValue(timestamp);
						}
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

    private TestingDataType(int jdbcType) {
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

    public void encode(Object o, MultiFieldEncoder encoder) {
        throw new UnsupportedOperationException();
    }

    public static BitSet getDoubleFields(TestingDataType... dataTypes) {
        BitSet bitSet = new BitSet(dataTypes.length);
        for(int i=0;i<dataTypes.length;i++){
            if(dataTypes[i]==TestingDataType.DOUBLE)
                bitSet.set(i);
        }
        return bitSet;
    }

    public static BitSet getFloatFields(TestingDataType... dataTypes) {
        BitSet bitSet = new BitSet(dataTypes.length);
        for(int i=0;i<dataTypes.length;i++){
            if(dataTypes[i]==TestingDataType.REAL)
                bitSet.set(i);
        }
        return bitSet;
    }

    public static BitSet getScalarFields(TestingDataType... dataTypes) {
        BitSet bitSet = new BitSet(dataTypes.length);
        for(int i=0;i<dataTypes.length;i++){
            if(dataTypes[i].isScalarType())
                bitSet.set(i);
        }
        return bitSet;
    }

    public static ExecRow getTemplateOutput(TestingDataType... outputDataTypes) {
        ExecRow valueRow = new ValueRow(outputDataTypes.length);
        int i=1;
        for(TestingDataType dataType:outputDataTypes){
            valueRow.setColumn(i,dataType.getDataValueDescriptor());
            i++;
        }
        return valueRow;
    }
}
