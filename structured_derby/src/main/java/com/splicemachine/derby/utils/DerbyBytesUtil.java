package com.splicemachine.derby.utils;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.BitSet;
import java.util.EnumMap;

import com.google.common.io.Closeables;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueDescriptor;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.ByteDataInput;
import com.splicemachine.utils.ByteDataOutput;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.utils.SpliceLogUtils;


public class DerbyBytesUtil {
	private static Logger LOG = Logger.getLogger(DerbyBytesUtil.class);

    private static final Serializer lazySerializer = new Serializer() {
        @Override
        public byte[] encode(DataValueDescriptor dvd) throws StandardException {
            return dvd.getBytes();
        }

        @Override
        public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
            LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor)dvd;
            ldvd.initForDeserialization(data);
        }

        @Override
        public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
            encoder.setRawBytes(((LazyDataValueDescriptor) dvd).getBytes(desc));
        }

        @Override
        public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
            LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor)dvd;
            int colFormatId = ldvd.getTypeFormatId();

            if(colFormatId== StoredFormatIds.SQL_REF_ID
                    ||colFormatId == StoredFormatIds.SQL_USERTYPE_ID_V3
                    ||colFormatId == StoredFormatIds.SQL_VARBIT_ID
                    ||colFormatId == StoredFormatIds.SQL_LONGVARBIT_ID
                    ||colFormatId == StoredFormatIds.SQL_BLOB_ID
                    ||colFormatId == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID
                    ||colFormatId == StoredFormatIds.SQL_BIT_ID){
                ((LazyDataValueDescriptor)dvd).initForDeserialization(decoder.getNextRawBytes(),desc);
            }else
                ((LazyDataValueDescriptor)dvd).initForDeserialization(decoder.getNextRaw(),desc);
        }

        @Override
        public boolean isScalarType() {
            throw new UnsupportedOperationException("Unable to get length from Lazy serializer");
        }
    };

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

	
	
	public static DataValueDescriptor fromBytes (byte[] bytes,
                                                 DataValueDescriptor descriptor) throws StandardException{
        if(bytes.length==0){
            descriptor.setToNull();
            return descriptor;
        }

        if(descriptor.isLazy()){
            lazySerializer.decode(bytes,descriptor);
            return descriptor;
        }
        Format format = Format.formatFor(descriptor);

        serializationMap.get(format).decode(bytes,descriptor);
        return descriptor;
    }

	public static byte[] generateBytes (DataValueDescriptor dvd) throws StandardException {
         /*
         * Don't bother to re-serialize HBaseRowLocations, they're already just bytes.
         */
        if(dvd==null||dvd.isNull()){
            return new byte[0];
        }
        if(dvd.isLazy())
            return lazySerializer.encode(dvd);

        Format format = Format.formatFor(dvd);
        return serializationMap.get(format).encode(dvd);
	}

	
	public static byte[] generateIncrementedScanKey(DataValueDescriptor[] descriptors, boolean[] sortOrder) throws IOException, StandardException {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(descriptors.length);
        return generateIncrementedScan(descriptors,encoder,sortOrder);
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
	}
	
	public static byte[] generateIndexKey(DataValueDescriptor[] descriptors, boolean[] sortOrder) throws IOException, StandardException {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(descriptors.length);
        /*
         * The last entry is a RowLocation (for indices). They must be sortable, but the default encoding
         * for RowLocations is unsorted. Thus, we have to be careful to encode any RowLocation values differently
         */
        for(int i=0;i<descriptors.length;i++){
            DataValueDescriptor dvd = descriptors[i];
            boolean desc = sortOrder!=null && !sortOrder[i];
            if(dvd.getTypeFormatId()==StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID){
                encoder = encoder.encodeNext(dvd.getBytes(),desc);
            }else
                encodeInto(encoder,dvd,desc);
        }
        return encoder.build();
	}

    public static MultiFieldEncoder encodeInto(MultiFieldEncoder encoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
        if(dvd.isLazy()){
            lazySerializer.encodeInto(dvd,encoder,desc);
            return encoder;
        }
        Format format = Format.formatFor(dvd);
        serializationMap.get(format).encodeInto(dvd,encoder,desc);
        return encoder;

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

    public static void decodeInto(MultiFieldDecoder rowDecoder, DataValueDescriptor column) throws StandardException{
        decodeInto(rowDecoder, column,false);
    }

    public static void decodeInto(MultiFieldDecoder rowDecoder, DataValueDescriptor column,boolean desc) throws StandardException{
        if(column.isLazy()){
            lazySerializer.decodeInto(column,rowDecoder,desc);
            return;
        }

        serializationMap.get(Format.formatFor(column)).decodeInto(column,rowDecoder,desc);
    }

    public static boolean isLengthDelimited(DataValueDescriptor descriptor) {
        if(descriptor==null) return false;

        return serializationMap.get(Format.formatFor(descriptor)).isScalarType();
    }

    private enum Format{
        BOOLEAN(StoredFormatIds.SQL_BOOLEAN_ID),
        TINYINT(StoredFormatIds.SQL_TINYINT_ID),
        SMALLINT(StoredFormatIds.SQL_SMALLINT_ID),
        INTEGER(StoredFormatIds.SQL_INTEGER_ID),
        LONGINT(StoredFormatIds.SQL_LONGINT_ID),
        REAL(StoredFormatIds.SQL_REAL_ID),
        DOUBLE(StoredFormatIds.SQL_DOUBLE_ID),
        DECIMAL(StoredFormatIds.SQL_DECIMAL_ID),
        REF(StoredFormatIds.SQL_REF_ID),
        USERTYPE(StoredFormatIds.SQL_USERTYPE_ID_V3),
        DATE(StoredFormatIds.SQL_DATE_ID),
        TIME(StoredFormatIds.SQL_TIME_ID),
        TIMESTAMP(StoredFormatIds.SQL_TIMESTAMP_ID),
        VARCHAR(StoredFormatIds.SQL_VARCHAR_ID),
        LONGVARCHAR(StoredFormatIds.SQL_LONGVARCHAR_ID),
        CLOB(StoredFormatIds.SQL_CLOB_ID),
        XML(StoredFormatIds.XML_ID),
        CHAR(StoredFormatIds.SQL_CHAR_ID),
        VARBIT(StoredFormatIds.SQL_VARBIT_ID),
        LONGVARBIT(StoredFormatIds.SQL_LONGVARBIT_ID),
        BLOB(StoredFormatIds.SQL_BLOB_ID),
        BIT(StoredFormatIds.SQL_BIT_ID),
        ROW_LOCATION(StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID);

        private final int storedFormatId;

        private Format(int storedFormatId) {
            this.storedFormatId = storedFormatId;
        }

        public static Format formatFor(DataValueDescriptor dvd){
            int typeFormatId = dvd.getTypeFormatId();
            for(Format format:values()){
                if(format.storedFormatId==typeFormatId){
                    return format;
                }
            }
            throw new IllegalArgumentException("Unable to determine format for dvd class"+ dvd.getClass());
        }
    }

    public static BitSet getScalarFields(DataValueDescriptor[] rowArray) {
        BitSet bitSet = new BitSet();
        for(int i=0;i<rowArray.length;i++){
            DataValueDescriptor dvd = rowArray[i];
            if(dvd!=null){
                Format format = Format.formatFor(dvd);
                Serializer serializer = serializationMap.get(format);
                if(serializer.isScalarType()){
                    bitSet.set(i);
                }
            }
        }
        return bitSet;
    }

    public static BitSet getFloatFields(DataValueDescriptor[] rowArray){
        BitSet bitSet = new BitSet();
        for(int i=0;i<rowArray.length;i++){
            DataValueDescriptor dvd = rowArray[i];
            if(dvd!=null){
                Format format = Format.formatFor(dvd);
                if(format==Format.REAL)
                    bitSet.set(i);
            }
        }
        return bitSet;
    }

    public static BitSet getDoubleFields(DataValueDescriptor[] rowArray){
        BitSet bitSet = new BitSet();
        for(int i=0;i<rowArray.length;i++){
            DataValueDescriptor dvd = rowArray[i];
            if(dvd!=null){
                Format format = Format.formatFor(dvd);
                if(format==Format.DOUBLE)
                    bitSet.set(i);
            }
        }
        return bitSet;
    }

    public static boolean isScalarType(DataValueDescriptor dvd) {
        if(dvd==null) return false;
        Format format = Format.formatFor(dvd);
        Serializer serializer = serializationMap.get(format);
        return serializer.isScalarType();
    }

    public static boolean isFloatType(DataValueDescriptor dvd){
        if(dvd==null) return false;
        return Format.formatFor(dvd)==Format.REAL;
    }

    public static boolean isDoubleType(DataValueDescriptor dvd){
        if(dvd==null) return false;
        return Format.formatFor(dvd)==Format.DOUBLE;
    }

    public static BitSet getNonNullFields(DataValueDescriptor[] row) {
        BitSet nonNullRows = new BitSet(row.length);
        for(int i=0;i<row.length;i++){
            DataValueDescriptor dvd = row[i];
            if(dvd!=null&&!dvd.isNull())
                nonNullRows.set(i);
        }
        return nonNullRows;
    }

    private interface Serializer{
        byte[] encode(DataValueDescriptor dvd) throws StandardException;

        void decode(byte[] data,DataValueDescriptor dvd) throws StandardException;

        void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException;

        void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException;

        boolean isScalarType();
    }

    private static EnumMap<Format,Serializer> serializationMap = new EnumMap<Format, Serializer>(Format.class);
    static{
        serializationMap.put(Format.BOOLEAN, new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getBoolean());
            }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(Encoding.decodeBoolean(data));
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getBoolean(),desc);
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextBoolean(desc));
            }

            @Override public boolean isScalarType() { return false;   }
        } );

        serializationMap.put(Format.DOUBLE, new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getDouble());
            }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(Encoding.decodeDouble(data));
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getDouble(),desc);
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextDouble(desc));
            }

            @Override public boolean isScalarType() { return false;   }
        });

        serializationMap.put(Format.TINYINT,new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getByte());
            }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(Encoding.decodeByte(data));
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getByte(),desc);
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextByte(desc));
            }

            @Override public boolean isScalarType() { return false;   }
        });

        serializationMap.put(Format.SMALLINT,new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getShort());
            }
            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(Encoding.decodeShort(data));
            }
            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getShort(),desc);
            }
            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextShort(desc));
            }

            @Override public boolean isScalarType() { return true;   }
        });

        serializationMap.put(Format.INTEGER,new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getInt());
            }
            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(Encoding.decodeInt(data));
            }
            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getInt(),desc);
            }
            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextInt(desc));
            }

            @Override public boolean isScalarType() { return true; }
        });

        serializationMap.put(Format.LONGINT,new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getLong());
            }
            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(Encoding.decodeLong(data));
            }
            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getLong(),desc);
            }
            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextLong(desc));
            }

            @Override public boolean isScalarType() { return true; }
        });

        serializationMap.put(Format.REAL,new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getFloat());
            }
            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(Encoding.decodeFloat(data));
            }
            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getFloat(),desc);
            }
            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextFloat(desc));
            }

            @Override public boolean isScalarType() { return false;   }
        });
         serializationMap.put(Format.DECIMAL,new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode((BigDecimal)dvd.getObject());
            }
            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setBigDecimal(Encoding.decodeBigDecimal(data));
            }
            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext((BigDecimal)dvd.getObject(),desc);
            }
            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setBigDecimal(decoder.decodeNextBigDecimal(desc));
            }

             @Override public boolean isScalarType() { return false; }
         });
        Serializer refSerializer = new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return getBytes(dvd,false);
            }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(getObject(data,false));
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNextUnsorted(getBytes(dvd,desc));
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                try{
                    ByteDataInput bdi = new ByteDataInput(decoder.decodeNextBytesUnsorted());
                    dvd.setValue(bdi.readObject());
                } catch (ClassNotFoundException e) {
                    throw Exceptions.parseException(e);
                } catch (IOException e) {
                    throw Exceptions.parseException(e);
                }
            }

            @Override public boolean isScalarType() { return false; }


            private Object getObject(byte[] data, boolean desc) throws StandardException{
                try{
                    ByteDataInput bdi = new ByteDataInput(Encoding.decodeBytesUnsortd(data,0,data.length));
                    return bdi.readObject();
                } catch (ClassNotFoundException e) {
                    throw Exceptions.parseException(e);
                } catch (IOException e) {
                    throw Exceptions.parseException(e);
                }
            }

            private byte[] getBytes(DataValueDescriptor dvd,boolean desc) throws StandardException{
                try{
                    ByteDataOutput bdo = new ByteDataOutput();
                    bdo.writeObject(dvd.getObject());
                    return bdo.toByteArray();
                }catch(IOException ioe){
                    throw Exceptions.parseException(ioe);
                }
            }
        };
        serializationMap.put(Format.REF,refSerializer);
        serializationMap.put(Format.USERTYPE,refSerializer);

        serializationMap.put(Format.DATE,new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getDate(null).getTime());
            }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(new Date(Encoding.decodeLong(data)));
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getDate(null).getTime(),desc);
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(new Date(decoder.decodeNextLong(desc)));
            }

            @Override public boolean isScalarType() { return true;   }
        });

        serializationMap.put(Format.TIME,new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getTime(null).getTime());
            }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(new Time(Encoding.decodeLong(data)));
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getTime(null).getTime(),desc);
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(new Time(decoder.decodeNextLong(desc)));
            }

            @Override public boolean isScalarType() { return true; }
        });
        serializationMap.put(Format.TIMESTAMP,new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getTimestamp(null).getTime());
            }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(new Timestamp(Encoding.decodeLong(data)));
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getTimestamp(null).getTime(),desc);
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(new Timestamp(decoder.decodeNextLong(desc)));
            }

            @Override public boolean isScalarType() { return true; }
        });

        Serializer stringSerializer = new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encode(dvd.getString());
            }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(Encoding.decodeString(data));
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNext(dvd.getString(),desc);
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextString(desc));
            }

            @Override public boolean isScalarType() { return false; }
        };
        serializationMap.put(Format.VARCHAR,stringSerializer);
        serializationMap.put(Format.LONGVARCHAR,stringSerializer);
        serializationMap.put(Format.CLOB,stringSerializer);
        serializationMap.put(Format.XML,stringSerializer);
        serializationMap.put(Format.CHAR,stringSerializer);

        Serializer byteSerializer = new Serializer() {
            @Override
            public byte[] encode(DataValueDescriptor dvd) throws StandardException {
                return Encoding.encodeBytesUnsorted(dvd.getBytes());
            }

            @Override
            public void decode(byte[] data, DataValueDescriptor dvd) throws StandardException {
                dvd.setValue(Encoding.decodeBytesUnsortd(data,0,data.length));
            }

            @Override
            public void encodeInto(DataValueDescriptor dvd, MultiFieldEncoder encoder, boolean desc) throws StandardException {
                encoder.encodeNextUnsorted(dvd.getBytes());
            }

            @Override
            public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
                dvd.setValue(decoder.decodeNextBytesUnsorted());
            }

            @Override public boolean isScalarType() { return false; }
        };
        serializationMap.put(Format.VARBIT,byteSerializer);
        serializationMap.put(Format.LONGVARBIT,byteSerializer);
        serializationMap.put(Format.BLOB,byteSerializer); //TODO -sf- this isn't going to be right for long
        serializationMap.put(Format.BIT,byteSerializer);
        serializationMap.put(Format.ROW_LOCATION,byteSerializer);
    }

    private static byte[] stripLength(byte[] data, boolean desc) {
        int length = Encoding.decodeInt(data, desc);
        int offset=1;
        for(int i=0;i<5;i++){
            if(data[length]==0x00){
                offset = i;
                break;
            }
        }
        byte[] actualData = new byte[data.length-offset];
        System.arraycopy(data,offset,actualData,0,data.length);
        return actualData;
    }


}
