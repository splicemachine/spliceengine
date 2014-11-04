package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.derby.impl.sql.execute.serial.DVDSerializer;
import com.splicemachine.derby.impl.sql.execute.serial.DecimalDVDSerializer;
import com.splicemachine.derby.impl.sql.execute.serial.DoubleDVDSerializer;
import com.splicemachine.derby.impl.sql.execute.serial.StringDVDSerializer;
import com.splicemachine.derby.impl.sql.execute.serial.TimestampDVDSerializer;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.utils.ByteSlice;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.types.*;

import java.math.BigDecimal;
import java.sql.Timestamp;

public class LazyDataValueFactory extends J2SEDataValueFactory{

    private static final StringDVDSerializer stringSerializer = new StringDVDSerializer();
    private static final DoubleDVDSerializer doubleSerializer = new DoubleDVDSerializer();
    private static final DecimalDVDSerializer decimalSerializer = new DecimalDVDSerializer();
    private static final TimestampDVDSerializer timestampSerializer = new TimestampDVDSerializer();

    public StringDataValue getVarcharDataValue(String value) {
        return new LazyStringDataValueDescriptor(new SQLVarchar(value));
    }

    public StringDataValue getVarcharDataValue(String value, StringDataValue previous) throws StandardException {

        StringDataValue result;

        if(previous instanceof LazyStringDataValueDescriptor){
            previous.setValue(value);
            result = previous;
        }else{
            if(previous != null){
                previous.setValue(value);
                result = new LazyStringDataValueDescriptor(previous);
            }else{
                result = new LazyStringDataValueDescriptor(new SQLVarchar(value));
            }
        }

        return result;
    }

    public StringDataValue getVarcharDataValue(String value, StringDataValue previous, int collationType) throws StandardException {
        return getVarcharDataValue(value, previous);
    }

    public DataValueDescriptor getNull(int formatId, int collationType) throws StandardException {
        return getLazyNull(formatId);
    }

    public NumberDataValue getDataValue(double value, NumberDataValue previous) throws StandardException {
        if(previous != null && previous instanceof LazyNumberDataValueDescriptor){
            previous.setValue(value);
        }else{
            previous = new LazyNumberDataValueDescriptor(new SQLDouble(value));
        }

        return previous;
    }

    public NumberDataValue getDataValue(Double value, NumberDataValue previous) throws StandardException {
        if(previous != null && previous instanceof LazyNumberDataValueDescriptor){
            previous.setValue(value);
        }else{
            previous = new LazyNumberDataValueDescriptor(new SQLDouble(value));
        }

        return previous;

    }

    public NumberDataValue getDecimalDataValue(Long value, NumberDataValue previous) throws StandardException {
        if(previous != null && previous instanceof LazyNumberDataValueDescriptor){
            previous.setValue(value);
        }else{
            previous = new LazyNumberDataValueDescriptor(new SQLDecimal(BigDecimal.valueOf(value.longValue())));
        }

        return previous;
    }

    public NumberDataValue getDecimalDataValue(String value) throws StandardException {
        return new LazyNumberDataValueDescriptor(new SQLDecimal(value));
    }

    public NumberDataValue getNullDecimal(NumberDataValue dataValue) {
        if(dataValue == null){
            dataValue = new LazyNumberDataValueDescriptor(new SQLDecimal());
        }else{
            dataValue.setToNull();
        }

        return dataValue;
    }

    @Override
    public DateTimeDataValue getDataValue(Timestamp value,
                                          DateTimeDataValue previous)
            throws StandardException
    {
        if (previous != null && previous instanceof LazyTimestampDataValueDescriptor) {
            previous.setValue(value);
        } else {
            previous = new LazyTimestampDataValueDescriptor(new SQLTimestamp(value));
        }
        return previous;
    }

    @Override
    public DateTimeDataValue getTimestampValue( String timestampStr, boolean isJdbcEscape) throws StandardException
    {
        SQLTimestamp ts = new SQLTimestamp( timestampStr, isJdbcEscape, getLocaleFinder());
        return new LazyTimestampDataValueDescriptor(ts);
    }

    @Override
    public DateTimeDataValue getTimestamp( DataValueDescriptor date, DataValueDescriptor time) throws StandardException
    {
        return new LazyTimestampDataValueDescriptor(new SQLTimestamp( date, time));
    }

    @Override
    public DateTimeDataValue getNullTimestamp(DateTimeDataValue dataValue)
    {
        if (dataValue == null)
        {
            try
            {
                return new LazyTimestampDataValueDescriptor(new SQLTimestamp((Timestamp) null));
            }
            catch( StandardException se)
            {
                return null;
            }
        }
        else
        {
            dataValue.setToNull();
            return dataValue;
        }
    }

    public static DataValueDescriptor getLazyNull(int formatId) throws StandardException {
        switch (formatId) {
        /* Wrappers */
            case StoredFormatIds.SQL_BIT_ID: return new SQLBit();
            case StoredFormatIds.SQL_BOOLEAN_ID: return new SQLBoolean();
            case StoredFormatIds.SQL_CHAR_ID: return new LazyStringDataValueDescriptor(new SQLChar());
            case StoredFormatIds.SQL_DATE_ID: return new SQLDate();
            case StoredFormatIds.SQL_DOUBLE_ID: return new LazyNumberDataValueDescriptor(new SQLDouble());
            case StoredFormatIds.SQL_DECIMAL_ID: return new LazyNumberDataValueDescriptor(new SQLDecimal());
            case StoredFormatIds.SQL_INTEGER_ID: return new SQLInteger();
            case StoredFormatIds.SQL_LONGINT_ID: return new SQLLongint();
            case StoredFormatIds.SQL_REAL_ID: return new SQLReal();
            case StoredFormatIds.SQL_REF_ID: return new SQLRef();
            case StoredFormatIds.SQL_SMALLINT_ID: return new SQLSmallint();
            case StoredFormatIds.SQL_TIME_ID: return new SQLTime();
            case StoredFormatIds.SQL_TIMESTAMP_ID: return new LazyTimestampDataValueDescriptor(new SQLTimestamp());
            case StoredFormatIds.SQL_TINYINT_ID: return new SQLTinyint();
            case StoredFormatIds.SQL_VARCHAR_ID: return new LazyStringDataValueDescriptor(new SQLVarchar());
            case StoredFormatIds.SQL_LONGVARCHAR_ID: return new LazyStringDataValueDescriptor(new SQLLongvarchar());
            case StoredFormatIds.SQL_VARBIT_ID: return new SQLVarbit();
            case StoredFormatIds.SQL_LONGVARBIT_ID: return new SQLLongVarbit();
            case StoredFormatIds.SQL_USERTYPE_ID_V3: return new UserType();
            case StoredFormatIds.SQL_BLOB_ID: return new SQLBlob();
            case StoredFormatIds.SQL_CLOB_ID: return new LazyStringDataValueDescriptor(new SQLClob());
            case StoredFormatIds.XML_ID: return new XML();
            default:return null;
        }
    }

    public static DVDSerializer getDVDSerializer(int formatId){
        switch (formatId) {
            case StoredFormatIds.SQL_CHAR_ID: return stringSerializer;
            case StoredFormatIds.SQL_DOUBLE_ID: return doubleSerializer;
            case StoredFormatIds.SQL_DECIMAL_ID: return decimalSerializer;
            case StoredFormatIds.SQL_VARCHAR_ID: return stringSerializer;
            case StoredFormatIds.SQL_LONGVARCHAR_ID: return stringSerializer;
            case StoredFormatIds.SQL_CLOB_ID: return stringSerializer;
            case StoredFormatIds.SQL_TIMESTAMP_ID: return timestampSerializer;
            default: throw new UnsupportedOperationException("No Serializer for format ID: " + formatId);
        }
    }

	@Override
	public DateTimeDataValue getDate(DataValueDescriptor operand) throws StandardException {
//		if (operand instanceof LazyTimestampDataValueDescriptor) {
//			DateTimeDataValue dtdv = this.getNullDate(null);
//			ByteSlice bs = ((LazyDataValueDescriptor) operand).bytes;
//			DerbyBytesUtil.decode(dtdv, bs.array(), bs.offset(), bs.length());
//			return dtdv;
//		}
		return super.getDate(operand);
	}
    
    
}
