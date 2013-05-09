package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.StringSerializer;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.types.*;

import javax.swing.*;

public class LazyDataValueFactory extends J2SEDataValueFactory{

    @Override
    public StringDataValue getVarcharDataValue(String value) {
        return new LazyStringDataValueDescriptor(new SQLVarchar(), new StringSerializer());
    }

    @Override
    public StringDataValue getVarcharDataValue(String value, StringDataValue previous) throws StandardException {

        StringDataValue result;

        if(previous instanceof LazyStringDataValueDescriptor){
            previous.setValue(value);
            result = previous;
        }else{
            if(previous != null){
                previous.setValue(value);
                result = new LazyStringDataValueDescriptor(previous, new StringSerializer());
            }else{
                SQLVarchar newDvd = new SQLVarchar();
                newDvd.setValue(value);
                result = new LazyStringDataValueDescriptor(newDvd, new StringSerializer());
            }
        }

        return result;
    }

    @Override
    public StringDataValue getVarcharDataValue(String value, StringDataValue previous, int collationType) throws StandardException {
        return getVarcharDataValue(value, previous);
    }

    public DataValueDescriptor getNull(int formatId, int collationType) throws StandardException {
        switch (formatId) {
        /* Wrappers */
            case StoredFormatIds.SQL_BIT_ID: return new SQLBit();
            case StoredFormatIds.SQL_BOOLEAN_ID: return new SQLBoolean();
            case StoredFormatIds.SQL_CHAR_ID: return new LazyStringDataValueDescriptor(new SQLChar(), new StringSerializer());
            case StoredFormatIds.SQL_DATE_ID: return new SQLDate();
            case StoredFormatIds.SQL_DOUBLE_ID: return new SQLDouble();
            case StoredFormatIds.SQL_INTEGER_ID: return new SQLInteger();
            case StoredFormatIds.SQL_LONGINT_ID: return new SQLLongint();
            case StoredFormatIds.SQL_REAL_ID: return new SQLReal();
            case StoredFormatIds.SQL_REF_ID: return new SQLRef();
            case StoredFormatIds.SQL_SMALLINT_ID: return new SQLSmallint();
            case StoredFormatIds.SQL_TIME_ID: return new SQLTime();
            case StoredFormatIds.SQL_TIMESTAMP_ID: return new SQLTimestamp();
            case StoredFormatIds.SQL_TINYINT_ID: return new SQLTinyint();
            case StoredFormatIds.SQL_VARCHAR_ID: return new LazyStringDataValueDescriptor(new SQLVarchar(), new StringSerializer());
            case StoredFormatIds.SQL_LONGVARCHAR_ID: return new LazyStringDataValueDescriptor(new SQLLongvarchar(), new StringSerializer());
            case StoredFormatIds.SQL_VARBIT_ID: return new SQLVarbit();
            case StoredFormatIds.SQL_LONGVARBIT_ID: return new SQLLongVarbit();
            case StoredFormatIds.SQL_USERTYPE_ID_V3: return new UserType();
            case StoredFormatIds.SQL_BLOB_ID: return new SQLBlob();
            case StoredFormatIds.SQL_CLOB_ID: return new LazyStringDataValueDescriptor(new SQLClob(), new StringSerializer());
            case StoredFormatIds.XML_ID: return new LazyDataValueDescriptor(new XML(), new StringSerializer());
            default:return null;
        }
    }
}
