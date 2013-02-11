package com.splicemachine.derby.utils;

import com.gotometrics.orderly.*;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * @author Scott Fines
 *         Created: 2/11/13 11:14 AM
 */
public class DescriptorSerializer {
	private RowKey longRowKey;
	private RowKey stringRowKey;
	private RowKey varByteArrayKey;
	private RowKey intRowKey;
	private RowKey floatRowKey;
	private RowKey decimalRowKey;
	private RowKey doubleRowKey;

	public void deserialize(DataValueDescriptor descriptor, byte[] source) throws IOException, StandardException {
		RowKey rowKey = null;
		switch(descriptor.getTypeFormatId()){
			case StoredFormatIds.SQL_DOUBLE_ID:
				descriptor.setValue(getDoubleRowKey().deserialize(source));
				return;
			case StoredFormatIds.SQL_SMALLINT_ID:
			case StoredFormatIds.SQL_INTEGER_ID:
				descriptor.setValue(getIntRowKey().deserialize(source));
				return;
			case StoredFormatIds.SQL_DATE_ID :
				descriptor.setValue(new Date((Long)getLongRowKey().deserialize(source)));
				return;
			case StoredFormatIds.SQL_TIMESTAMP_ID:
				descriptor.setValue(new Timestamp((Long)getLongRowKey().deserialize(source)));
				return;
			case StoredFormatIds.SQL_TIME_ID:
				descriptor.setValue(new Time((Long)getLongRowKey().deserialize(source)));
				return;
			case StoredFormatIds.SQL_VARCHAR_ID: //return new SQLVarchar();
			case StoredFormatIds.SQL_LONGVARCHAR_ID: //return new SQLLongvarchar();
			case StoredFormatIds.SQL_CLOB_ID: //return new SQLClob();
			case StoredFormatIds.XML_ID: //return new XML();
			case StoredFormatIds.SQL_CHAR_ID: //return new SQLChar();
				descriptor.setValue(getStringRowKey().deserialize(source));
				return;
			case StoredFormatIds.SQL_VARBIT_ID: //return new SQLVarbit();
			case StoredFormatIds.SQL_LONGVARBIT_ID: //return new SQLLongVarbit();
			case StoredFormatIds.SQL_BLOB_ID: //return new SQLBlob();
			case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID:
			case StoredFormatIds.SQL_BIT_ID: //return new SQLBit();
				descriptor.setValue(getVarByteArrayKey().deserialize(source));
				return;
			case StoredFormatIds.SQL_DECIMAL_ID:
				descriptor.setValue(getDecimalRowKey().deserialize(source));
				return;
			default:
				throw StandardException.newException(SQLState.LANG_TYPE_NOT_SERIALIZABLE,descriptor.getClass());
		}
	}

	public byte[] serialize(DataValueDescriptor dvd) throws StandardException, IOException {
		switch(dvd.getTypeFormatId()){
			case StoredFormatIds.SQL_DOUBLE_ID:
				return getDoubleRowKey().serialize(dvd.getDouble());

			case StoredFormatIds.SQL_SMALLINT_ID:
			case StoredFormatIds.SQL_INTEGER_ID:
				return getIntRowKey().serialize(dvd.getInt());

			case StoredFormatIds.SQL_DATE_ID:
				return getLongRowKey().serialize(dvd.getDate(null).getTime());
			case StoredFormatIds.SQL_LONGINT_ID:
				return getLongRowKey().serialize(dvd.getLong());
			case StoredFormatIds.SQL_TIME_ID:
				return getLongRowKey().serialize(dvd.getTime(null).getTime());
			case StoredFormatIds.SQL_TIMESTAMP_ID:
				return getLongRowKey().serialize(dvd.getTimestamp(null).getTime());

			case StoredFormatIds.SQL_REAL_ID:
				getFloatRowKey().serialize(dvd.getFloat());

			case StoredFormatIds.SQL_BOOLEAN_ID:
				return getVarByteArrayKey().serialize(Bytes.toBytes(dvd.getBoolean()));
			case StoredFormatIds.SQL_VARBIT_ID:
			case StoredFormatIds.SQL_LONGVARBIT_ID:
			case StoredFormatIds.SQL_BLOB_ID:
			case StoredFormatIds.SQL_BIT_ID:
			case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID:
				return getVarByteArrayKey().serialize(dvd.getBytes());
			case StoredFormatIds.SQL_TINYINT_ID:
				return getVarByteArrayKey().serialize(Bytes.toBytes(dvd.getByte()));
			case StoredFormatIds.SQL_REF_ID:
			case StoredFormatIds.SQL_USERTYPE_ID_V3:
				return getVarByteArrayKey().serialize(objectToBytes(dvd.getObject()));

			case StoredFormatIds.SQL_VARCHAR_ID:
			case StoredFormatIds.SQL_LONGVARCHAR_ID:
			case StoredFormatIds.SQL_CLOB_ID:
			case StoredFormatIds.XML_ID:
			case StoredFormatIds.SQL_CHAR_ID:
				return getStringRowKey().serialize(dvd.getString());

			case StoredFormatIds.SQL_DECIMAL_ID:
				return getDecimalRowKey().serialize(dvd.getObject());
			default:
				throw StandardException.newException(SQLState.LANG_TYPE_NOT_SERIALIZABLE,dvd.getClass());
		}
	}

	private RowKey getDecimalRowKey() {
		if(decimalRowKey==null) decimalRowKey = new BigDecimalRowKey();
		return decimalRowKey;
	}

	private RowKey getStringRowKey() {
		if(stringRowKey==null)stringRowKey = new NullRemovingRowKey();
		return stringRowKey;
	}

	private RowKey getFloatRowKey() {
		if(floatRowKey==null)floatRowKey = new FloatRowKey();
		return floatRowKey;
	}

	private RowKey getLongRowKey() {
		if(longRowKey ==null) longRowKey = new LongRowKey();
		return longRowKey;
	}

	private RowKey getIntRowKey() {
		if(intRowKey==null)intRowKey = new IntegerRowKey();
		return intRowKey;
	}

	private RowKey getDoubleRowKey() {
		if(doubleRowKey==null) doubleRowKey = new DoubleRowKey();
		return doubleRowKey;
	}

	private RowKey getVarByteArrayKey() {
		if(varByteArrayKey==null)varByteArrayKey = new VariableLengthByteArrayRowKey();
		return varByteArrayKey;
	}

	private Object objectToBytes(Object object) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(object);
		return baos.toByteArray();
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
			if(replacedString.length()<=0)
				return null;
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
