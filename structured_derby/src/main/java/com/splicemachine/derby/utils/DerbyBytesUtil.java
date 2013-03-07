package com.splicemachine.derby.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import com.gotometrics.orderly.*;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.utils.SpliceLogUtils;


public class DerbyBytesUtil {
	private static Logger LOG = Logger.getLogger(DerbyBytesUtil.class);

	public static DataValueDescriptor fromBytes (byte[] bytes, DataValueDescriptor descriptor) throws StandardException, IOException {
        //TODO -sf- move this into the Serializer abstraction
		SpliceLogUtils.trace(LOG,"fromBytes %s",descriptor.getTypeName());
        /*
         * Because HBaseRowLocations are just byte[] row keys, there's no reason to re-serialize them, they've
         * already been serialized and compacted.
         */
        if(descriptor instanceof HBaseRowLocation){
            ((HBaseRowLocation)descriptor).setValue(bytes);
            return descriptor;
        }
		try {
			switch (descriptor.getTypeFormatId()) {
		    	case StoredFormatIds.SQL_BOOLEAN_ID: //return new SQLBoolean();
		    		descriptor.setValue(Bytes.toBoolean((byte[])getRowKey(descriptor).deserialize(bytes)));
		    	    break;
		    	case StoredFormatIds.SQL_DATE_ID: //return new SQLDate();
		    		 descriptor.setValue(new Date((Long) getRowKey(descriptor).deserialize(bytes)));
		    	    break;
		    	case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
						descriptor.setValue(((BigDecimal)getRowKey(descriptor).deserialize(bytes)).doubleValue());
		    	    break;
				case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
		    	case StoredFormatIds.SQL_INTEGER_ID: //return new SQLInteger();
		    		descriptor.setValue(((Integer)getRowKey(descriptor).deserialize(bytes)).intValue());
		    	    break;
		    	case StoredFormatIds.SQL_LONGINT_ID: //return new SQLLongint();
		    		descriptor.setValue(((Long)getRowKey(descriptor).deserialize(bytes)).longValue());
		    	    break;
		    	case StoredFormatIds.SQL_REAL_ID: //return new SQLReal();
		    		descriptor.setValue(((Float)getRowKey(descriptor).deserialize(bytes)).floatValue());
		    	    break;
		    	case StoredFormatIds.SQL_REF_ID: //return new SQLRef();
		    	case StoredFormatIds.SQL_USERTYPE_ID_V3: //return new UserType();
		    		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
					ObjectInputStream ois = new ObjectInputStream(bis);
					descriptor.setValue(ois.readObject());
					ois.close();
					bis.close();
					break;
		    	case StoredFormatIds.SQL_TINYINT_ID: //return new SQLTinyint();
		    		descriptor.setValue(Bytes.toShort((byte[])getRowKey(descriptor).deserialize(bytes)));
		    	    break;
		    	case StoredFormatIds.SQL_TIME_ID: //return new SQLTime();
		    		 descriptor.setValue(new Time((Long)getRowKey(descriptor).deserialize(bytes)));
		    	    break;
		    	case StoredFormatIds.SQL_TIMESTAMP_ID: //return new SQLTimestamp();
		    		 descriptor.setValue(new Timestamp((Long)getRowKey(descriptor).deserialize(bytes)));
		    	    break;
		    	case StoredFormatIds.SQL_VARCHAR_ID: //return new SQLVarchar();
		    	case StoredFormatIds.SQL_LONGVARCHAR_ID: //return new SQLLongvarchar();
		    	case StoredFormatIds.SQL_CLOB_ID: //return new SQLClob();
		    	case StoredFormatIds.XML_ID: //return new XML();
		    	case StoredFormatIds.SQL_CHAR_ID: //return new SQLChar();
		    		descriptor.setValue((String)getRowKey(descriptor).deserialize(bytes));
		    	    break;
		    	case StoredFormatIds.SQL_VARBIT_ID: //return new SQLVarbit();
		    	case StoredFormatIds.SQL_LONGVARBIT_ID: //return new SQLLongVarbit();
		    	case StoredFormatIds.SQL_BLOB_ID: //return new SQLBlob();
		    	case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID:
		    	case StoredFormatIds.SQL_BIT_ID: //return new SQLBit();
		    		descriptor.setValue((byte[])getRowKey(descriptor).deserialize(bytes));
		    	    break;
		    	case StoredFormatIds.SQL_DECIMAL_ID:
		    		descriptor.setBigDecimal((BigDecimal)getRowKey(descriptor).deserialize(bytes));
		    	    break;
		        default:
                    LOG.error("Byte array generation failed " + descriptor.getClass());
                    throw new RuntimeException("Attempt to serialize an unimplemented serializable object " + descriptor.getClass());
            }
            return descriptor;
        } catch (Exception e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Byte array generation failed " + descriptor.getClass() + ":"+e.getMessage(),e);
            //won't happen, because the above method will throw a runtime exception
            return null;
        }
    }
		
	public static byte[] generateBytes (DataValueDescriptor descriptor) throws StandardException, IOException {
        //TODO -sf- move this into the Serializer abstraction
        /*
         * Don't bother to re-serialize HBaseRowLocations, they're already just bytes.
         */
        if(descriptor instanceof HBaseRowLocation){
            return ((HBaseRowLocation)descriptor).getBytes();
        }
		//SpliceLogUtils.trace(LOG,"generateBytes for descriptor %s with value %s",descriptor,descriptor.getTraceString());
		try {
			switch (descriptor.getTypeFormatId()) {
		    	case StoredFormatIds.SQL_BOOLEAN_ID: //return new SQLBoolean();
		    		return getRowKey(descriptor).serialize(Bytes.toBytes(descriptor.getBoolean()));
		    	case StoredFormatIds.SQL_DATE_ID: //return new SQLDate();
		    		return getRowKey(descriptor).serialize(descriptor.getTimestamp(null).getTime());
		    	case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
		    		return getRowKey(descriptor).serialize(BigDecimal.valueOf(descriptor.getDouble()));
				case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
		    	case StoredFormatIds.SQL_INTEGER_ID: //return new SQLInteger();
		    		return getRowKey(descriptor).serialize(descriptor.getInt());
		    	case StoredFormatIds.SQL_LONGINT_ID: //return new SQLLongint();
		    		return getRowKey(descriptor).serialize(descriptor.getLong());
		    	case StoredFormatIds.SQL_REAL_ID: //return new SQLReal();
		    		return getRowKey(descriptor).serialize(descriptor.getFloat());
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
		    		return getRowKey(descriptor).serialize(descriptor.getTime(null).getTime());
		    	case StoredFormatIds.SQL_TIMESTAMP_ID: //return new SQLTimestamp();
		    		return getRowKey(descriptor).serialize(descriptor.getTimestamp(null).getTime());
		    	case StoredFormatIds.SQL_TINYINT_ID: //return new SQLTinyint();
		    		return getRowKey(descriptor).serialize(Bytes.toBytes(descriptor.getByte()));
		    	case StoredFormatIds.SQL_VARCHAR_ID: //return new SQLVarchar();
		    	case StoredFormatIds.SQL_LONGVARCHAR_ID: //return new SQLLongvarchar();
		    	case StoredFormatIds.SQL_CLOB_ID: //return new SQLClob();
		    	case StoredFormatIds.XML_ID: //return new XML();
		    	case StoredFormatIds.SQL_CHAR_ID: //return new SQLChar();
		    		return getRowKey(descriptor).serialize(descriptor.getString());
		    	case StoredFormatIds.SQL_VARBIT_ID: //return new SQLVarbit();
		    	case StoredFormatIds.SQL_LONGVARBIT_ID: //return new SQLLongVarbit();
		    	case StoredFormatIds.SQL_BLOB_ID: //return new SQLBlob();
		    	case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID:
		    	case StoredFormatIds.SQL_BIT_ID: //return new SQLBit();
		    		return getRowKey(descriptor).serialize(descriptor.getBytes());
		    	case StoredFormatIds.SQL_DECIMAL_ID:
		    		return getRowKey(descriptor).serialize((BigDecimal) descriptor.getObject());
		        default:
		        	throw new RuntimeException("Attempt to serialize an unimplemented serializable object " + descriptor.getClass());
			}
		} catch (Exception e) {
				SpliceLogUtils.logAndThrowRuntime(LOG,"Byte array generation failed ",e);
				return null;
		}
	}

	
	public static byte[] generateIncrementedScanKey(DataValueDescriptor[] descriptors, boolean[] sortOrder) throws IOException, StandardException {
		StructBuilder builder = new StructBuilder();
		Object[] values = new Object[descriptors.length];
		for (int i=0;i<descriptors.length;i++) {
			RowKey rowKey = getRowKey(descriptors[i]);
			if (sortOrder != null && !sortOrder[i])
				rowKey.setOrder(Order.DESCENDING);
			builder.add(rowKey);
			if (i == descriptors.length - 1) {
				descriptors[i].getObject();
				if (rowKey.getSerializedClass() == byte[].class) {
					byte[] test = (byte[])descriptors[i].cloneValue(true).getObject();
					byte[] newArray = new byte[test.length];
					System.arraycopy(test, 0, newArray, 0, test.length);					
					BytesUtil.incrementAtIndex(newArray, newArray.length - 1);
					values[i] = newArray;
				}
				else if(rowKey.getSerializedClass() == Double.class) {
					Double test = (Double)descriptors[i].cloneValue(true).getObject();
					test = test + Double.longBitsToDouble(0x1L);
					values[i] = test;					
				}
				else if (rowKey.getSerializedClass() == Long.class) {
					Long test = (Long)descriptors[i].cloneValue(true).getObject();
					test = test + 1l;
					values[i] = test;										
				}
				else if (rowKey.getSerializedClass() == Integer.class) {
					Integer test = (Integer)descriptors[i].cloneValue(true).getObject();
					test = test + 1;
					values[i] = test;										
				}
				else if (rowKey.getSerializedClass() == Float.class) {
					Float test = (Float)descriptors[i].cloneValue(true).getObject();
					test = test + Float.MIN_VALUE;
					values[i] = test;										
				}
				else if (rowKey.getSerializedClass() == String.class) {
					String test = (String)descriptors[i].cloneValue(true).getObject();
					byte[] testByteArray = Bytes.toBytes(test);
//                    if(testByteArray.length != 0)
                        BytesUtil.incrementAtIndex(testByteArray, testByteArray.length - 1);
//                    else
//                        testByteArray = new byte[]{(byte)1};
					values[i] = Bytes.toString(testByteArray);										
				}
				else if (rowKey.getSerializedClass() == BigDecimal.class) {
					BigDecimal test = (BigDecimal)descriptors[i].cloneValue(true).getObject();
					test = test.add(new BigDecimal(Double.MIN_VALUE)); // hack
					values[i] = test;										
				}
				else {
					throw new RuntimeException("Row Key Type not Supported " + rowKey.getSerializedClass());
				}
				
			} else {
				values[i] = descriptors[i].getObject();
			}
				
		}
		return builder.toRowKey().serialize(values);
	}

	public static byte[] generateSortedHashKey(DataValueDescriptor[] descriptors, 
											 DataValueDescriptor uniqueString, 
											 int[] hash_keys, 
											 boolean[] sortOrder) throws IOException, StandardException {
		SpliceLogUtils.debug(LOG, "generateSortedHashKey");
		StructBuilder builder = new StructBuilder();
		Object[] values = new Object[hash_keys.length+2];
		RowKey rowKey;
		rowKey = getRowKey(uniqueString);
		builder.add(rowKey);
		values[0] = uniqueString.getObject();
		SpliceLogUtils.trace(LOG, "generateSortedHashKey#uniqueString " + values[0]);
		for (int i=0;i<hash_keys.length;i++) {
			rowKey = getRowKey(descriptors[hash_keys[i]]);
			if (sortOrder != null && !sortOrder[hash_keys[i]])
				rowKey.setOrder(Order.DESCENDING);
			builder.add(rowKey);
			values[i+1] = descriptors[hash_keys[i]].getObject();
			SpliceLogUtils.trace(LOG, "generateSortedHashKey#iteration value" + values[i+1]);
		}
		values[hash_keys.length+1] = SpliceUtils.getUniqueKey();
		builder.add(new VariableLengthByteArrayRowKey());		
		return builder.toRowKey().serialize(values);
	}
	
	public static byte[] generateBeginKeyForTemp(DataValueDescriptor uniqueString) throws StandardException, IOException {
		SpliceLogUtils.trace(LOG,"generateBeginKeyForTemp is %s",uniqueString.getTraceString());
		StructBuilder builder = new StructBuilder();
		Object[] values = new Object[1];
		RowKey rowKey;
		rowKey = getRowKey(uniqueString);
		builder.add(rowKey);
		values[0] = uniqueString.getString();
		return builder.toRowKey().serialize(values);
	}

	public static byte[] generateEndKeyForTemp(DataValueDescriptor uniqueString) throws StandardException, IOException {
		SpliceLogUtils.trace(LOG,"generateEndKeyForTemp is %s",uniqueString.getTraceString());
		byte[] bytes= generateBeginKeyForTemp(uniqueString);
		BytesUtil.incrementAtIndex(bytes,bytes.length-1);

		RowKey rowKey = new VariableLengthByteArrayRowKey();
		return rowKey.serialize(bytes);
	}

	
	public static byte[] generateIncrementedSortedHashScan(Qualifier[][] qualifiers, DataValueDescriptor uniqueString) throws IOException, StandardException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("generateIncrementedSortedHashScan with Qualifiers " + qualifiers + ", with unique String " + uniqueString);
			for (int j = 0; j<qualifiers[0].length;j++) {
				LOG.trace("Qualifier: " + qualifiers[0][j].getOrderable().getTraceString());
			}
		}
		StructBuilder builder = new StructBuilder();
		Object[] values = new Object[qualifiers[0].length+1];
		RowKey rowKey;
		rowKey = getRowKey(uniqueString);
		builder.add(rowKey);
		values[0] = uniqueString.getString();
		for(int i = 0;i<qualifiers[0].length;i++) { //Qualifier q: qualifiers[0]){
			Qualifier q = qualifiers[0][i];
			rowKey = getRowKey(q.getOrderable());
			builder.add(rowKey);
			if(i == qualifiers[0].length-1){
				LOG.trace("Incremented Value: " + q.getOrderable().getTraceString());				
				if(rowKey.getSerializedClass() == byte[].class) {
					byte[] test = (byte[])q.getOrderable().getObject();
					byte[] copy = new byte[test.length];
					System.arraycopy(test,0,copy,0,test.length);
					BytesUtil.incrementAtIndex(copy,copy.length-1);
					values[i+1] = copy;
				}else if(rowKey.getSerializedClass() == Double.class){
					Double t = (Double)q.getOrderable().getObject();
					t = t + Double.longBitsToDouble(0x1L);
					values[i+1] = t;
				}else if(rowKey.getSerializedClass() == Long.class){
					Long t = (Long)q.getOrderable().getObject();
					t = t+1l;
					values[i+1] = t;
				}else if(rowKey.getSerializedClass() == Integer.class){
					values[i+1] = (Integer)q.getOrderable().getObject()+1;
				}else if(rowKey.getSerializedClass() == Float.class){
					values[i+1] = (Float)q.getOrderable().getObject()+Float.MIN_VALUE;
				}else if (rowKey.getSerializedClass()==String.class){
					String t = (String)q.getOrderable().getObject();
					byte[] bytes = Bytes.toBytes(t);
					BytesUtil.incrementAtIndex(bytes,bytes.length-1);
					values[i+1] = Bytes.toString(bytes);
				}else if (rowKey.getSerializedClass()==BigDecimal.class){
					values[i+1] = ((BigDecimal)q.getOrderable().getObject()).add(new BigDecimal(Double.MIN_VALUE));
				}else
					throw new RuntimeException("Unable to parse key class "+rowKey.getSerializedClass());
			}else {
				LOG.trace("Created Value: " + q.getOrderable().getTraceString());	
				values[i+1] = q.getOrderable().getObject();
			}
		}
		
		return builder.toRowKey().serialize(values);
	}

	/*
	 * Note: This will only work with GenericScanQualifiers, *not* with
	 * other qualifier types.
	 */
	public static byte[] generateSortedHashScan(Qualifier[][] qualifiers, DataValueDescriptor uniqueString) throws IOException, StandardException {
		SpliceLogUtils.debug(LOG, "generateSortedHashScan");
		if (LOG.isTraceEnabled()) {
			LOG.trace("generateSortedHashScan with Qualifiers " + qualifiers + ", with unique String " + uniqueString);
			for (int j = 0; j<qualifiers[0].length;j++) {
				LOG.trace("Qualifier: " + qualifiers[0][j].getOrderable().getTraceString());
			}
		}
		StructBuilder builder = new StructBuilder();
		Object[] values = new Object[qualifiers[0].length+1];
		RowKey rowKey;
		rowKey = getRowKey(uniqueString);
		builder.add(rowKey);
		values[0] = uniqueString.getString();
		SpliceLogUtils.trace(LOG, "generateSortedHashScan#uniqueString " + values[0]);
		for (int i=0;i<qualifiers[0].length;i++) {
			LOG.trace("Created Value: " + qualifiers[0][i].getOrderable().getTraceString());				
			rowKey = getRowKey(qualifiers[0][i].getOrderable());
			builder.add(rowKey);
			values[i+1] = qualifiers[0][i].getOrderable().getObject();
			SpliceLogUtils.trace(LOG, "generateSortedHashScan#iteration value " + values[i+1]);
		}
		return builder.toRowKey().serialize(values);
	}
	
	public static byte[] generateIndexKey(DataValueDescriptor[] descriptors, boolean[] sortOrder) throws IOException, StandardException {
		StructBuilder builder = new StructBuilder();
		Object[] values = new Object[descriptors.length];
		for (int i=0;i<descriptors.length;i++) {
			RowKey rowKey = getRowKey(descriptors[i]);
			if (sortOrder != null && !sortOrder[i])
				rowKey.setOrder(Order.DESCENDING);
			builder.add(rowKey);
			values[i] = descriptors[i].getObject();
		}
		return builder.toRowKey().serialize(values);
	}
	
	public static byte[] generatePrefixedRowKey(DataValueDescriptor uniqueString) throws IOException, StandardException {
		StructBuilder builder = new StructBuilder();
		Object[] values = new Object[2];
		values[0] = uniqueString.getObject();
		builder.add(getRowKey(uniqueString));
		values[1] = SpliceUtils.getUniqueKey();
		builder.add(new VariableLengthByteArrayRowKey());	
		return builder.toRowKey().serialize(values);
	}
		
	public static byte[] generateScanKeyForIndex(DataValueDescriptor[] startKeyValue,int startSearchOperator, boolean[] sortOrder) throws IOException, StandardException {
        if(startKeyValue==null)return null;
		switch(startSearchOperator) { // public static final int GT = -1;
            case ScanController.NA:
            case ScanController.GE:
                return generateIndexKey(startKeyValue,sortOrder);
            case ScanController.GT:
                return generateIncrementedScanKey(startKeyValue,sortOrder);
            default:
                throw new RuntimeException("Error with Key Generation");
		}
	}

	public static RowKey getRowKey(DataValueDescriptor descriptor) {
		switch (descriptor.getTypeFormatId()) {
	    	case StoredFormatIds.SQL_BOOLEAN_ID: //return new SQLBoolean();
	    	case StoredFormatIds.SQL_REF_ID: //return new SQLRef();
	    	case StoredFormatIds.SQL_USERTYPE_ID_V3: //return new UserType();
	    	case StoredFormatIds.SQL_TINYINT_ID: //return new SQLTinyint();
	    	case StoredFormatIds.SQL_VARBIT_ID: //return new SQLVarbit();
	    	case StoredFormatIds.SQL_LONGVARBIT_ID: //return new SQLLongVarbit();
	    	case StoredFormatIds.SQL_BLOB_ID: //return new SQLBlob();
	    	case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID:
	    	case StoredFormatIds.SQL_BIT_ID: //return new SQLBit();	
	    		return new VariableLengthByteArrayRowKey();
	    	case StoredFormatIds.SQL_DATE_ID: //return new SQLDate();
	    	case StoredFormatIds.SQL_LONGINT_ID: //return new SQLLongint();
	    	case StoredFormatIds.SQL_TIME_ID: //return new SQLTime();
	    	case StoredFormatIds.SQL_TIMESTAMP_ID: //return new SQLTimestamp();
	    		return new LongRowKey();
			case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
	    	case StoredFormatIds.SQL_INTEGER_ID: //return new SQLInteger();
	    		return new IntegerRowKey();
	    	case StoredFormatIds.SQL_REAL_ID: //return new SQLReal();
	    		return new FloatRowKey();
	    	case StoredFormatIds.SQL_VARCHAR_ID: //return new SQLVarchar();
	    	case StoredFormatIds.SQL_LONGVARCHAR_ID: //return new SQLLongvarchar();
	    	case StoredFormatIds.SQL_CLOB_ID: //return new SQLClob();
	    	case StoredFormatIds.XML_ID: //return new XML();
	    	case StoredFormatIds.SQL_CHAR_ID: //return new SQLChar();
					return new NullRemovingRowKey();
			case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
			case StoredFormatIds.SQL_DECIMAL_ID:
					return new BigDecimalRowKey();
	        default:
	        	throw new RuntimeException("Attempt to serialize an unimplemented serializable object " + descriptor.getClass());
		}
	}

    public static Object getObject(DataValueDescriptor descriptor) throws StandardException {
        switch(descriptor.getTypeFormatId()){
            case StoredFormatIds.SQL_TIMESTAMP_ID:
                return descriptor.getTimestamp(null).getTime();
            case StoredFormatIds.SQL_DATE_ID:
                return descriptor.getDate(null).getTime();
            case StoredFormatIds.SQL_TIME_ID:
                return descriptor.getTime(null).getTime();
			case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
                return descriptor.getInt();
			case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
                return BigDecimal.valueOf(descriptor.getDouble());
	        default:
                return descriptor.getObject();
        }
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
