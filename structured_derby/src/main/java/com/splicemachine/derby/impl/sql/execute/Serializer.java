package com.splicemachine.derby.impl.sql.execute;

import com.gotometrics.orderly.*;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @author Scott Fines
 *         Created on: 2/25/13
 */
public class Serializer {
    private RowKey longRowKey;
    private RowKey intRowKey;
    private RowKey bytesRowKey;
    private RowKey decimalRowKey;
    private RowKey floatRowKey;
    private RowKey stringRowKey;

    public byte[] serialize(DataValueDescriptor descriptor) throws IOException, StandardException {
        if (descriptor.isNull()) {
            return new byte[] {};
        }
        if(descriptor instanceof HBaseRowLocation){
            return ((HBaseRowLocation)descriptor).getBytes();
        }
        switch(descriptor.getTypeFormatId()){
            case StoredFormatIds.SQL_BOOLEAN_ID: //return new SQLBoolean();
                return getByteRowKey().serialize(Bytes.toBytes(descriptor.getBoolean()));
            case StoredFormatIds.SQL_DATE_ID: //return new SQLDate();
                return getLongRowKey().serialize(descriptor.getTimestamp(null).getTime());
            case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
                return getDecimalRowKey().serialize(BigDecimal.valueOf(descriptor.getDouble()));
            case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
            case StoredFormatIds.SQL_INTEGER_ID: //return new SQLInteger();
                return getIntegerRowKey().serialize(descriptor.getInt());
            case StoredFormatIds.SQL_LONGINT_ID: //return new SQLLongint();
                return getLongRowKey().serialize(descriptor.getLong());
            case StoredFormatIds.SQL_REAL_ID: //return new SQLReal();
                return getFloatRowKey().serialize(descriptor.getFloat());
            case StoredFormatIds.SQL_REF_ID: //return new SQLRef();
            case StoredFormatIds.SQL_USERTYPE_ID_V3: //return new UserType();
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(descriptor.getObject());
                byte[] out = bos.toByteArray();
                oos.flush();
                bos.flush();
                oos.close();
                bos.close();
                return out;
//		    		return getRowKey(descriptor).serialize(Bytes.toBytes(descriptor.getShort()));
            case StoredFormatIds.SQL_TIME_ID: //return new SQLTime();
                return getLongRowKey().serialize(descriptor.getTime(null).getTime());
            case StoredFormatIds.SQL_TIMESTAMP_ID: //return new SQLTimestamp();
                return getLongRowKey().serialize(descriptor.getTimestamp(null).getTime());
            case StoredFormatIds.SQL_TINYINT_ID: //return new SQLTinyint();
                return getByteRowKey().serialize(Bytes.toBytes(descriptor.getByte()));
            case StoredFormatIds.SQL_VARCHAR_ID: //return new SQLVarchar();
            case StoredFormatIds.SQL_LONGVARCHAR_ID: //return new SQLLongvarchar();
            case StoredFormatIds.SQL_CLOB_ID: //return new SQLClob();
            case StoredFormatIds.XML_ID: //return new XML();
            case StoredFormatIds.SQL_CHAR_ID: //return new SQLChar();
                return getStringRowKey().serialize(descriptor.getString());
            case StoredFormatIds.SQL_VARBIT_ID: //return new SQLVarbit();
            case StoredFormatIds.SQL_LONGVARBIT_ID: //return new SQLLongVarbit();
            case StoredFormatIds.SQL_BLOB_ID: //return new SQLBlob();
            case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID:
            case StoredFormatIds.SQL_BIT_ID: //return new SQLBit();
                return getByteRowKey().serialize(descriptor.getBytes());
            case StoredFormatIds.SQL_DECIMAL_ID:
                return getDecimalRowKey().serialize(descriptor.getObject());
            default:
                throw new IOException("Attempt to serialize an unimplemented serializable object " + descriptor.getClass());
        }
    }

    public DataValueDescriptor deserialize(byte[] bytes, DataValueDescriptor descriptor) throws StandardException,IOException{
        if(descriptor instanceof HBaseRowLocation){
            ((HBaseRowLocation)descriptor).setValue(bytes);
        }
        if (bytes.length == 0) {
            descriptor.setToNull();
            return descriptor;
        }
        switch (descriptor.getTypeFormatId()) {
            case StoredFormatIds.SQL_BOOLEAN_ID: //return new SQLBoolean();
                descriptor.setValue(Bytes.toBoolean((byte[])getByteRowKey().deserialize(bytes)));
                break;
            case StoredFormatIds.SQL_DATE_ID: //return new SQLDate();
                descriptor.setValue(new Date((Long) getLongRowKey().deserialize(bytes)));
                break;
            case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
                descriptor.setValue(((BigDecimal) getDecimalRowKey().deserialize(bytes)).doubleValue());
                break;
            case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
            case StoredFormatIds.SQL_INTEGER_ID: //return new SQLInteger();
                descriptor.setValue(((Integer) getIntegerRowKey().deserialize(bytes)).intValue());
                break;
            case StoredFormatIds.SQL_LONGINT_ID: //return new SQLLongint();
                descriptor.setValue(((Long) getLongRowKey().deserialize(bytes)).longValue());
                break;
            case StoredFormatIds.SQL_REAL_ID: //return new SQLReal();
                descriptor.setValue(((Float) getFloatRowKey().deserialize(bytes)).floatValue());
                break;
            case StoredFormatIds.SQL_REF_ID: //return new SQLRef();
            case StoredFormatIds.SQL_USERTYPE_ID_V3: //return new UserType();
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                try {
                    descriptor.setValue(ois.readObject());
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }
                ois.close();
                bis.close();
                break;
            case StoredFormatIds.SQL_TINYINT_ID: //return new SQLTinyint();
                descriptor.setValue(Bytes.toShort((byte[])getByteRowKey().deserialize(bytes)));
                break;
            case StoredFormatIds.SQL_TIME_ID: //return new SQLTime();
                descriptor.setValue(new Time((Long)getLongRowKey().deserialize(bytes)));
                break;
            case StoredFormatIds.SQL_TIMESTAMP_ID: //return new SQLTimestamp();
                descriptor.setValue(new Timestamp((Long)getLongRowKey().deserialize(bytes)));
                break;
            case StoredFormatIds.SQL_VARCHAR_ID: //return new SQLVarchar();
            case StoredFormatIds.SQL_LONGVARCHAR_ID: //return new SQLLongvarchar();
            case StoredFormatIds.SQL_CLOB_ID: //return new SQLClob();
            case StoredFormatIds.XML_ID: //return new XML();
            case StoredFormatIds.SQL_CHAR_ID: //return new SQLChar();
                descriptor.setValue((String)getStringRowKey().deserialize(bytes));
                break;
            case StoredFormatIds.SQL_VARBIT_ID: //return new SQLVarbit();
            case StoredFormatIds.SQL_LONGVARBIT_ID: //return new SQLLongVarbit();
            case StoredFormatIds.SQL_BLOB_ID: //return new SQLBlob();
            case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID:
            case StoredFormatIds.SQL_BIT_ID: //return new SQLBit();
                descriptor.setValue((byte[])getByteRowKey().deserialize(bytes));
                break;
            case StoredFormatIds.SQL_DECIMAL_ID:
                descriptor.setBigDecimal((BigDecimal)getDecimalRowKey().deserialize(bytes));
                break;
            default:
                throw new IOException("Attempt to serialize an unimplemented serializable object " + descriptor.getClass());
        }
        return descriptor;
    }


    private RowKey getStringRowKey() {
        if(stringRowKey==null)stringRowKey = new NullRemovingRowKey();
        return stringRowKey;
    }

    private RowKey getFloatRowKey() {
        if(floatRowKey == null) floatRowKey = new FloatRowKey();
        return floatRowKey;
    }

    private RowKey getIntegerRowKey() {
        if(intRowKey==null)intRowKey = new IntegerRowKey();
        return intRowKey;
    }

    private RowKey getDecimalRowKey() {
        if(decimalRowKey==null)decimalRowKey = new BigDecimalRowKey();
        return decimalRowKey;
    }

    private RowKey getLongRowKey() {
        if(longRowKey==null)longRowKey = new LongRowKey();
        return longRowKey;
    }

    private RowKey getByteRowKey() {
        if(bytesRowKey ==null) bytesRowKey = new VariableLengthByteArrayRowKey();
        return bytesRowKey;
    }

    /**
     * String RowKey which trims off extraneous whitespace and empty characters before serializing.
     */
    private static class NullRemovingRowKey extends UTF8RowKey {

        @Override public Class<?> getSerializedClass() { return String.class; }

        @Override
        public int getSerializedLength(Object o) throws IOException {
            return super.getSerializedLength(toUTF8(o));
        }

        private Object toUTF8(Object o) {
            if(o==null|| o instanceof byte[]) return o;
            String replacedString = o.toString().replaceAll("\u0000","");
//				if(replacedString.length()<=0)
//					return null;
            return Bytes.toBytes(replacedString);
        }

        @Override
        public void serialize(Object o, ImmutableBytesWritable w) throws IOException {
            super.serialize(toUTF8(o),w);
        }

        @Override
        public Object deserialize(ImmutableBytesWritable w) throws IOException {
            byte[] b = (byte[])super.deserialize(w);
            return b ==null? b :  Bytes.toString(b);
        }
    }

}
