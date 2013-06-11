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
import com.google.common.io.Closeables;
//import com.gotometrics.orderly.*;
import com.gotometrics.orderly.*;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueDescriptor;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
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

	@SuppressWarnings("unchecked")
	public static <T> T fromBytes(byte[] bytes, Class<T> instanceClass) throws StandardException {
        ByteDataInput bdi = null;
		try {
            bdi = new ByteDataInput(bytes);
			return (T) bdi.readObject();
		} catch (Exception e) {
			Closeables.closeQuietly(bdi);
            SpliceLogUtils.logAndThrow(LOG,"fromBytes Exception",Exceptions.parseException(e));
            return null; //can't happen
		}
	}

	public static byte[] toBytes(Object object) throws StandardException {
        ByteDataOutput bdo = null;
		try {
            bdo = new ByteDataOutput();
			bdo.writeObject(object);
			return bdo.toByteArray();
		} catch (Exception e) {
			Closeables.closeQuietly(bdo);
            SpliceLogUtils.logAndThrow(LOG,"fromBytes Exception",Exceptions.parseException(e));
            return null;
		}
	}

	
	
	public static DataValueDescriptor fromBytes (byte[] bytes, DataValueDescriptor descriptor) throws StandardException, IOException {
        //TODO -sf- move this into the Serializer abstraction
        /*
         * Because HBaseRowLocations are just byte[] row keys, there's no reason to re-serialize them, they've
         * already been serialized and compacted.
         */
        if(descriptor.getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID){
            descriptor.setValue(bytes);
            return descriptor;
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
        try {
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
					descriptor.setValue(bdi.readObject());
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

    public static byte[] generateBytes (DataValueDescriptor dvd,boolean stripNullStrings) throws StandardException, IOException {
       /*
         * Don't bother to re-serialize HBaseRowLocations, they're already just bytes.
         */
        if(dvd.getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID
                || dvd.isLazy()){
            return dvd.getBytes();
        } else {
            switch(dvd.getTypeFormatId()){
                case StoredFormatIds.SQL_BOOLEAN_ID: //return new SQLBoolean();
                    return Encoding.encode(dvd.getBoolean());
                case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
                    return Encoding.encode(dvd.getDouble());
                case StoredFormatIds.SQL_TINYINT_ID: //return new SQLTinyint();
                    return Encoding.encode(dvd.getByte());
                case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
                    return Encoding.encode(dvd.getShort());
                case StoredFormatIds.SQL_INTEGER_ID: //return new SQLInteger();
                    return Encoding.encode(dvd.getInt());
                case StoredFormatIds.SQL_LONGINT_ID: //return new SQLLongint();
                    return Encoding.encode(dvd.getLong());
                case StoredFormatIds.SQL_REAL_ID: //return new SQLReal();
                    return Encoding.encode(dvd.getFloat());
                case StoredFormatIds.SQL_REF_ID: //return new SQLRef();
                case StoredFormatIds.SQL_USERTYPE_ID_V3: //return new UserType();
                    ByteDataOutput bdo = new ByteDataOutput();
                    bdo.writeObject(dvd.getObject());
                    return Encoding.encode(bdo.toByteArray());
                case StoredFormatIds.SQL_DATE_ID: //return new SQLDate();
                    return Encoding.encode(dvd.getDate(null).getTime());
                case StoredFormatIds.SQL_TIME_ID: //return new SQLTime();
                    return Encoding.encode(dvd.getTime(null).getTime());
                case StoredFormatIds.SQL_TIMESTAMP_ID: //return new SQLTimestamp();
                    return Encoding.encode(dvd.getTimestamp(null).getTime());
                case StoredFormatIds.SQL_VARCHAR_ID: //return new SQLVarchar();
                case StoredFormatIds.SQL_LONGVARCHAR_ID: //return new SQLLongvarchar();
                case StoredFormatIds.SQL_CLOB_ID: //return new SQLClob();
                case StoredFormatIds.XML_ID: //return new XML();
                case StoredFormatIds.SQL_CHAR_ID: //return new SQLChar();
                    String value = dvd.getString();
                    if(stripNullStrings)
                        return Encoding.encodeNullFree(value);
                    else
                        return Encoding.encode(value);
                case StoredFormatIds.SQL_VARBIT_ID: //return new SQLVarbit();
                case StoredFormatIds.SQL_LONGVARBIT_ID: //return new SQLLongVarbit(); TODO -sf- LONGVARBIT does not allow comparisons, so no need to do a variable binary encoding
                case StoredFormatIds.SQL_BLOB_ID: //return new SQLBlob();
                case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID:
                case StoredFormatIds.SQL_BIT_ID: //return new SQLBit();
                    return Encoding.encode(dvd.getBytes());
                case StoredFormatIds.SQL_DECIMAL_ID:
                    return Encoding.encode((BigDecimal) dvd.getObject());
                default:
                    throw StandardException.newException (SQLState.DATA_UNEXPECTED_EXCEPTION,"Attempt to serialize an unimplemented serializable object " + dvd.getClass());
            }
        }
    }

	public static byte[] generateBytes (DataValueDescriptor dvd) throws StandardException, IOException {
        //TODO -sf- move this into the Serializer abstraction
        return generateBytes(dvd,false);
	}

	
	public static byte[] generateIncrementedScanKey(DataValueDescriptor[] descriptors, boolean[] sortOrder) throws IOException, StandardException {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(descriptors.length);
        return generateIncrementedScan(descriptors,encoder,sortOrder);
//		StructBuilder builder = new StructBuilder();
//		Object[] values = new Object[descriptors.length];
//		for (int i=0;i<descriptors.length;i++) {
//			RowKey rowKey = getRowKey(descriptors[i]);
//			if (sortOrder != null && !sortOrder[i])
//				rowKey.setOrder(Order.DESCENDING);
//			builder.add(rowKey);
//			if (i == descriptors.length - 1) {
//				descriptors[i].getObject();
//				if (rowKey.getSerializedClass() == byte[].class) {
//					byte[] test = (byte[])descriptors[i].cloneValue(true).getObject();
//					byte[] newArray = new byte[test.length];
//					System.arraycopy(test, 0, newArray, 0, test.length);
//					BytesUtil.incrementAtIndex(newArray, newArray.length - 1);
//					values[i] = newArray;
//				}
//				else if(rowKey.getSerializedClass() == Double.class) {
//					Double test = (Double)descriptors[i].cloneValue(true).getObject();
//					test = test + Double.longBitsToDouble(0x1L);
//					values[i] = test;
//				}
//				else if (rowKey.getSerializedClass() == Long.class) {
//					Long test = (Long)descriptors[i].cloneValue(true).getObject();
//					test = test + 1l;
//					values[i] = test;
//				}
//				else if (rowKey.getSerializedClass() == Integer.class) {
//					Integer test = (Integer)descriptors[i].cloneValue(true).getObject();
//					test = test + 1;
//					values[i] = test;
//				}
//				else if (rowKey.getSerializedClass() == Float.class) {
//					Float test = (Float)descriptors[i].cloneValue(true).getObject();
//					test = test + Float.MIN_VALUE;
//					values[i] = test;
//				}
//				else if (rowKey.getSerializedClass() == String.class) {
//					String test = (String)descriptors[i].cloneValue(true).getObject();
//					byte[] testByteArray = Bytes.toBytes(test);
//                    BytesUtil.incrementAtIndex(testByteArray, testByteArray.length - 1);
//					values[i] = Bytes.toString(testByteArray);
//				}
//				else if (rowKey.getSerializedClass() == BigDecimal.class) {
//					BigDecimal test = (BigDecimal)descriptors[i].cloneValue(true).getObject();
//					test = test.add(new BigDecimal(Double.MIN_VALUE)); // hack
//					values[i] = test;
//				}
//				else {
//					throw new RuntimeException("Row Key Type not Supported " + rowKey.getSerializedClass());
//				}
//
//			} else {
//				values[i] = descriptors[i].getObject();
//			}
//
//		}
//		return builder.toRowKey().serialize(values);
	}

    public static byte[] generateBeginKeyForTemp(DataValueDescriptor uniqueString) throws StandardException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"generateBeginKeyForTemp is %s",uniqueString.getTraceString());
        return Encoding.encode(uniqueString.getString());
    }

	public static byte[] generateIncrementedSortedHashScan(Qualifier[][] qualifiers, DataValueDescriptor uniqueString) throws IOException, StandardException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("generateIncrementedSortedHashScan with Qualifiers " + qualifiers + ", with unique String " + uniqueString);
			for (int j = 0; j<qualifiers[0].length;j++) {
				LOG.trace("Qualifier: " + qualifiers[0][j].getOrderable().getTraceString());
			}
		}
        MultiFieldEncoder encoder = MultiFieldEncoder.create(qualifiers[0].length+1);
        encoder.encodeNext(uniqueString.getString(),false);
        DataValueDescriptor[] dvds = new DataValueDescriptor[qualifiers[0].length];
        for(int pos=0;pos<qualifiers[0].length;pos++){
            dvds[pos] = qualifiers[0][pos].getOrderable();
        }
        return generateIncrementedScan(dvds, encoder,null);
//		StructBuilder builder = new StructBuilder();
//		Object[] values = new Object[qualifiers[0].length+1];
//		RowKey rowKey;
//		rowKey = getRowKey(uniqueString);
//		builder.add(rowKey);
//		values[0] = uniqueString.getString();
//		for(int i = 0;i<qualifiers[0].length;i++) { //Qualifier q: qualifiers[0]){
//			Qualifier q = qualifiers[0][i];
//			rowKey = getRowKey(q.getOrderable());
//			builder.add(rowKey);
//			if(i == qualifiers[0].length-1){
////				if(rowKey.getSerializedClass() == byte[].class) {
////					byte[] test = (byte[])q.getOrderable().getObject();
////					byte[] copy = new byte[test.length];
////					System.arraycopy(test,0,copy,0,test.length);
////					BytesUtil.incrementAtIndex(copy,copy.length-1);
////					values[i+1] = copy;
//				}else if(rowKey.getSerializedClass() == Double.class){
//					Double t = (Double)q.getOrderable().getObject();
//					t = t + Double.longBitsToDouble(0x1L);
//					values[i+1] = t;
//				}else if(rowKey.getSerializedClass() == Long.class){
//					Long t = (Long)q.getOrderable().getObject();
//					t = t+1l;
//					values[i+1] = t;
//				}else if(rowKey.getSerializedClass() == Integer.class){
//					values[i+1] = (Integer)q.getOrderable().getObject()+1;
//				}else if(rowKey.getSerializedClass() == Float.class){
//					values[i+1] = (Float)q.getOrderable().getObject()+Float.MIN_VALUE;
////				}else if (rowKey.getSerializedClass()==String.class){
////					String t = (String)q.getOrderable().getObject();
////					byte[] bytes = Bytes.toBytes(t);
////					BytesUtil.incrementAtIndex(bytes,bytes.length-1);
////					values[i+1] = Bytes.toString(bytes);
//////				}else if (rowKey.getSerializedClass()==BigDecimal.class){
////					values[i+1] = ((BigDecimal)q.getOrderable().getObject()).add(new BigDecimal(Double.MIN_VALUE));
//				}else
//					throw new RuntimeException("Unable to parse key class "+rowKey.getSerializedClass());
//			}else {
//				if (LOG.isTraceEnabled())
//					LOG.trace("Created Value: " + q.getOrderable().getTraceString());
//				values[i+1] = q.getOrderable().getObject();
//			}
//		}
//
//		return builder.toRowKey().serialize(values);
	}

    private static byte[] generateIncrementedScan(DataValueDescriptor[] dvds, MultiFieldEncoder encoder,boolean[] sortOrder) throws StandardException, IOException {
        for(int i=0;i< dvds.length;i++){
            DataValueDescriptor dvd = dvds[i];
            boolean desc = sortOrder!=null && !sortOrder[i];
            if(i!= dvds.length-1){
                encodeInto(encoder,dvd,false);
            }else{
                boolean longSet = false;
                long l = Long.MAX_VALUE;
                switch(dvd.getTypeFormatId()){
                    case StoredFormatIds.SQL_BOOLEAN_ID: //return new SQLBoolean();
                        //in ascending order, false is after true, so make it false to catch everything
                        encoder = encoder.encodeNext(false,desc);
                        break;
                    case StoredFormatIds.SQL_DATE_ID: //return new SQLDate();
                        l = dvd.getDate(null).getTime();
                        longSet=true;
                    case StoredFormatIds.SQL_TIME_ID: //return new SQLTime();
                        if(!longSet){
                            l = dvd.getTime(null).getTime();
                            longSet=true;
                        }
                    case StoredFormatIds.SQL_TIMESTAMP_ID: //return new SQLTimestamp();
                        if(!longSet){
                            l = dvd.getTimestamp(null).getTime();
                            longSet=true;
                        }
                    case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
                    case StoredFormatIds.SQL_TINYINT_ID: //return new SQLTinyint();
                    case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
                    case StoredFormatIds.SQL_INTEGER_ID: //return new SQLInteger();
                    case StoredFormatIds.SQL_LONGINT_ID: //return new SQLLongint();
                        if(!longSet)
                            l = dvd.getLong();
                        /*
                         * We have to watch out for overflows here, since incrementing Long.MAX_VALUE
                         * will result in incorrect positioning. However, we don't have to worry too much,
                         * since we can just encode Long.MAX_VALUE and it'll compare >= everything else.
                         */
                        if(l<Long.MAX_VALUE){
                            l+=1l; //TODO -sf- we're just going to have to hope that this doesn't overflow
                        }
                        encoder = encoder.encodeNext(l,desc); //we can't go over without overflow,
                        break;
                    case StoredFormatIds.SQL_REAL_ID: //return new SQLReal();
                        float f = dvd.getFloat()+Float.MIN_VALUE;
                        encoder = encoder.encodeNext(f,desc);
                        break;
                    case StoredFormatIds.SQL_VARCHAR_ID: //return new SQLVarchar();
                    case StoredFormatIds.SQL_LONGVARCHAR_ID: //return new SQLLongvarchar();
                    case StoredFormatIds.SQL_CLOB_ID: //return new SQLClob();
                    case StoredFormatIds.XML_ID: //return new XML();
                    case StoredFormatIds.SQL_CHAR_ID: //return new SQLChar();
                        String t = dvd.getString();
                        byte[] bytes = Bytes.toBytes(t);
                        BytesUtil.incrementAtIndex(bytes, bytes.length - 1);
                        encoder = encoder.encodeNext(Bytes.toString(bytes),desc);
                        break;
                    case StoredFormatIds.SQL_VARBIT_ID: //return new SQLVarbit();
                    case StoredFormatIds.SQL_LONGVARBIT_ID: //return new SQLLongVarbit();
                    case StoredFormatIds.SQL_BLOB_ID: //return new SQLBlob();
                    case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID:
                    case StoredFormatIds.SQL_BIT_ID: //return new SQLBit();
                        byte[] data = dvd.getBytes();
                        BytesUtil.copyAndIncrement(data);
                        encoder = encoder.encodeNext(data,desc);
                        break;
                    case StoredFormatIds.SQL_DECIMAL_ID:
                        BigDecimal value = new BigDecimal(Double.MIN_VALUE).add((BigDecimal)dvd.getObject());
                        encoder = encoder.encodeNext(value,desc);
                        break;
                    default:
                        throw new IOException("Unable to sort on field " + dvd.getClass()+",type="+dvd.getTypeName());
                }
            }
        }
        return encoder.build();
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
        MultiFieldEncoder encoder = MultiFieldEncoder.create(qualifiers[0].length+1);
        encoder = encoder.encodeNext(uniqueString.getString());
        for(int i=0;i<qualifiers[0].length;i++){
            encodeInto(encoder,qualifiers[0][i].getOrderable(),false);
        }
        return encoder.build();
//		StructBuilder builder = new StructBuilder();
//		Object[] values = new Object[qualifiers[0].length+1];
//		RowKey rowKey;
//		rowKey = getRowKey(uniqueString);
//		builder.add(rowKey);
//		values[0] = uniqueString.getString();
//		if (LOG.isTraceEnabled())
//			SpliceLogUtils.trace(LOG, "generateSortedHashScan#uniqueString " + values[0]);
//		for (int i=0;i<qualifiers[0].length;i++) {
//			rowKey = getRowKey(qualifiers[0][i].getOrderable());
//			builder.add(rowKey);
//			values[i+1] = qualifiers[0][i].getOrderable().getObject();
//			SpliceLogUtils.trace(LOG, "generateSortedHashScan#iteration value %s",values[i+1]);
//		}
//		return builder.toRowKey().serialize(values);
	}
	
	public static byte[] generateIndexKey(DataValueDescriptor[] descriptors, boolean[] sortOrder) throws IOException, StandardException {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(descriptors.length);
        for(int i=0;i<descriptors.length;i++){
            DataValueDescriptor dvd = descriptors[i];
            boolean desc = sortOrder!=null && !sortOrder[i];
            encodeInto(encoder,dvd,desc);
        }
        return encoder.build();
//		StructBuilder builder = new StructBuilder();
//		Object[] values = new Object[descriptors.length];
//		for (int i=0;i<descriptors.length;i++) {
//			RowKey rowKey = getRowKey(descriptors[i]);
//			if (sortOrder != null && !sortOrder[i])
//				rowKey.setOrder(Order.DESCENDING);
//			builder.add(rowKey);
//			values[i] = descriptors[i].getObject();
//		}
//		return builder.toRowKey().serialize(values);
	}

    public static MultiFieldEncoder encodeInto(MultiFieldEncoder encoder, DataValueDescriptor dvd, boolean desc) throws StandardException, IOException {
        switch(dvd.getTypeFormatId()){
            case StoredFormatIds.SQL_BOOLEAN_ID: //return new SQLBoolean();
                return encoder.encodeNext(dvd.getBoolean(),desc);
            case StoredFormatIds.SQL_DOUBLE_ID: //return new SQLDouble();
                return encoder.encodeNext(dvd.getDouble(), desc);
            case StoredFormatIds.SQL_TINYINT_ID: //return new SQLTinyint();
                return encoder.encodeNext(dvd.getByte(), desc);
            case StoredFormatIds.SQL_SMALLINT_ID: //return new SQLSmallint();
                return encoder.encodeNext(dvd.getShort(), desc);
            case StoredFormatIds.SQL_INTEGER_ID: //return new SQLInteger();
                return encoder.encodeNext(dvd.getInt(), desc);
            case StoredFormatIds.SQL_LONGINT_ID: //return new SQLLongint();
                return encoder.encodeNext(dvd.getLong(), desc);
            case StoredFormatIds.SQL_REAL_ID: //return new SQLReal();
                return encoder.encodeNext(dvd.getFloat(), desc);
            case StoredFormatIds.SQL_REF_ID: //return new SQLRef();
            case StoredFormatIds.SQL_USERTYPE_ID_V3: //return new UserType();
                ByteDataOutput bdo = new ByteDataOutput();
                bdo.writeObject(dvd.getObject());
                return encoder.encodeNext(bdo.toByteArray(), desc);
            case StoredFormatIds.SQL_DATE_ID: //return new SQLDate();
                return encoder.encodeNext(dvd.getDate(null).getTime(), desc);
            case StoredFormatIds.SQL_TIME_ID: //return new SQLTime();
                return encoder.encodeNext(dvd.getTime(null).getTime(), desc);
            case StoredFormatIds.SQL_TIMESTAMP_ID: //return new SQLTimestamp();
                return encoder.encodeNext(dvd.getTimestamp(null).getTime(), desc);
            case StoredFormatIds.SQL_VARCHAR_ID: //return new SQLVarchar();
            case StoredFormatIds.SQL_LONGVARCHAR_ID: //return new SQLLongvarchar();
            case StoredFormatIds.SQL_CLOB_ID: //return new SQLClob();
            case StoredFormatIds.XML_ID: //return new XML();
            case StoredFormatIds.SQL_CHAR_ID: //return new SQLChar();
                return encoder.encodeNext(dvd.getString(),desc);
            case StoredFormatIds.SQL_VARBIT_ID: //return new SQLVarbit();
            case StoredFormatIds.SQL_LONGVARBIT_ID: //return new SQLLongVarbit(); TODO -sf- LONGVARBIT does not allow comparisons, so no need to do a variable binary encoding
            case StoredFormatIds.SQL_BLOB_ID: //return new SQLBlob();
            case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID:
            case StoredFormatIds.SQL_BIT_ID: //return new SQLBit();
                return encoder.encodeNext(dvd.getBytes(), desc);
            case StoredFormatIds.SQL_DECIMAL_ID:
                return encoder.encodeNext((BigDecimal) dvd.getObject(), desc);
            default:
                throw new IOException("Attempt to serialize an unimplemented serializable object " + dvd.getClass());
        }
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

    /**
     * String RowKey which trims off extraneous whitespace and empty characters before serializing.
     */
    public static class NullRemovingRowKey extends UTF8RowKey {

        private static final char NULL_CHAR = '\u0000';

			@Override public Class<?> getSerializedClass() { return String.class; }

			@Override
			public int getSerializedLength(Object o) throws IOException {
				return super.getSerializedLength(toUTF8(o));
			}

            private String stripChar(String s, char c){
                StringBuilder strBuilder = new StringBuilder(s.length());

                for(char stringChar : s.toCharArray()){
                    if( stringChar != c){
                        strBuilder.append(stringChar);
                    }
                }

                return strBuilder.toString();
            }

			private Object toUTF8(Object o) {
				if(o==null|| o instanceof byte[]) return o;

                String objectString = o.toString();

                String replacedString;

                if( objectString.indexOf(NULL_CHAR) != -1){
                    replacedString = stripChar(objectString, NULL_CHAR);
                } else {
                    replacedString = objectString;
                }

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
