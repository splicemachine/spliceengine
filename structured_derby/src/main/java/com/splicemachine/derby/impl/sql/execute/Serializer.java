package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.utils.ByteDataInput;
import com.splicemachine.utils.ByteDataOutput;
import com.splicemachine.encoding.Encoding;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @author Scott Fines
 *         Created on: 2/25/13
 */
public class Serializer {
    private static final Serializer INSTANCE = new Serializer();

    private Serializer(){}

    public static Serializer get(){
        return INSTANCE;
    }

    public byte[] serialize(DataValueDescriptor descriptor) throws IOException, StandardException {
        if (descriptor.isNull()) {
            return new byte[] {};
        }
        if (descriptor.getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID
                || descriptor.isLazy()){
            return descriptor.getBytes();
        }

        switch(descriptor.getTypeFormatId()){
            case StoredFormatIds.SQL_BOOLEAN_ID: //return new SQLBoolean();
                return Encoding.encode(descriptor.getBoolean());
            case StoredFormatIds.SQL_DATE_ID: //return new SQLDate();
                return Encoding.encode(descriptor.getDate(null).getTime());
            case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
                return Encoding.encode(descriptor.getDouble());
            case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
                return Encoding.encode(descriptor.getShort());
            case StoredFormatIds.SQL_INTEGER_ID: //return new SQLInteger();
                return Encoding.encode(descriptor.getInt());
            case StoredFormatIds.SQL_LONGINT_ID: //return new SQLLongint();
                return Encoding.encode(descriptor.getLong());
            case StoredFormatIds.SQL_REAL_ID: //return new SQLReal();
                return Encoding.encode(descriptor.getFloat());
            case StoredFormatIds.SQL_REF_ID: //return new SQLRef();
            case StoredFormatIds.SQL_USERTYPE_ID_V3: //return new UserType();
                ByteDataOutput bdo = new ByteDataOutput();
                bdo.writeObject(descriptor.getObject());
                return Encoding.encode(bdo.toByteArray());
            case StoredFormatIds.SQL_TIME_ID: //return new SQLTime();
                return Encoding.encode(descriptor.getTime(null).getTime());
            case StoredFormatIds.SQL_TIMESTAMP_ID: //return new SQLTimestamp();
                return Encoding.encode(descriptor.getTimestamp(null).getTime());
            case StoredFormatIds.SQL_TINYINT_ID: //return new SQLTinyint();
                return Encoding.encode(descriptor.getByte());
            case StoredFormatIds.SQL_VARCHAR_ID: //return new SQLVarchar();
            case StoredFormatIds.SQL_LONGVARCHAR_ID: //return new SQLLongvarchar();
            case StoredFormatIds.SQL_CLOB_ID: //return new SQLClob();
            case StoredFormatIds.XML_ID: //return new XML();
            case StoredFormatIds.SQL_CHAR_ID: //return new SQLChar();
                return Encoding.encode(descriptor.getString());
            case StoredFormatIds.SQL_VARBIT_ID: //return new SQLVarbit();
            case StoredFormatIds.SQL_LONGVARBIT_ID: //return new SQLLongVarbit(); TODO -sf- LONGVARBIT does not allow comparisons, so no need to do a variable binary encoding
            case StoredFormatIds.SQL_BLOB_ID: //return new SQLBlob();
            case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID:
            case StoredFormatIds.SQL_BIT_ID: //return new SQLBit();
                return Encoding.encode(descriptor.getBytes());
            case StoredFormatIds.SQL_DECIMAL_ID:
                return Encoding.encode((BigDecimal) descriptor.getObject());
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

        if(descriptor.isLazy()){
            LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor) descriptor;
            ldvd.initForDeserialization(bytes);
            return ldvd;
        }

        switch (descriptor.getTypeFormatId()) {
            case StoredFormatIds.SQL_BOOLEAN_ID: //return new SQLBoolean();
                descriptor.setValue(Encoding.decodeBoolean(bytes));
                break;
            case StoredFormatIds.SQL_DATE_ID: //return new SQLDate();
                descriptor.setValue(new Date(Encoding.decodeLong(bytes)));
                break;
            case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
                descriptor.setValue(Encoding.decodeDouble(bytes));
                break;
            case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
                descriptor.setValue(Encoding.decodeShort(bytes));
            case StoredFormatIds.SQL_INTEGER_ID: //return new SQLInteger();
                descriptor.setValue(Encoding.decodeInt(bytes));
                break;
            case StoredFormatIds.SQL_LONGINT_ID: //return new SQLLongint();
                descriptor.setValue(Encoding.decodeLong(bytes));
                break;
            case StoredFormatIds.SQL_REAL_ID: //return new SQLReal();
                descriptor.setValue(Encoding.decodeFloat(bytes));
                break;
            case StoredFormatIds.SQL_REF_ID: //return new SQLRef();
            case StoredFormatIds.SQL_USERTYPE_ID_V3: //return new UserType();
                ByteDataInput bdi = new ByteDataInput(Encoding.decodeBytes(bytes));
                try {
                    descriptor.setValue(bdi.readObject());
                } catch (ClassNotFoundException e) {
                    throw new IOException(e);
                }
                break;
            case StoredFormatIds.SQL_TINYINT_ID: //return new SQLTinyint();
                descriptor.setValue(Encoding.decodeByte(bytes));
                break;
            case StoredFormatIds.SQL_TIME_ID: //return new SQLTime();
                descriptor.setValue(new Time(Encoding.decodeLong(bytes)));
                break;
            case StoredFormatIds.SQL_TIMESTAMP_ID: //return new SQLTimestamp();
                descriptor.setValue(new Timestamp(Encoding.decodeLong(bytes)));
                break;
            case StoredFormatIds.SQL_VARCHAR_ID: //return new SQLVarchar();
            case StoredFormatIds.SQL_LONGVARCHAR_ID: //return new SQLLongvarchar();
            case StoredFormatIds.SQL_CLOB_ID: //return new SQLClob();
            case StoredFormatIds.XML_ID: //return new XML();
            case StoredFormatIds.SQL_CHAR_ID: //return new SQLChar();
                descriptor.setValue(Encoding.decodeString(bytes));
                break;
            case StoredFormatIds.SQL_VARBIT_ID: //return new SQLVarbit();
            case StoredFormatIds.SQL_LONGVARBIT_ID: //return new SQLLongVarbit();
            case StoredFormatIds.SQL_BLOB_ID: //return new SQLBlob();
            case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID:
            case StoredFormatIds.SQL_BIT_ID: //return new SQLBit();
                descriptor.setValue(Encoding.decodeBytes(bytes));
                break;
            case StoredFormatIds.SQL_DECIMAL_ID:
                descriptor.setBigDecimal(Encoding.decodeBigDecimal(bytes));
                break;
            default:
                throw new IOException("Attempt to serialize an unimplemented serializable object " + descriptor.getClass());
        }
        return descriptor;
    }

}
