package com.splicemachine.derby.utils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Calendar;
import com.carrotsearch.hppc.BitSet;
import com.google.common.io.Closeables;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.ByteDataInput;
import com.splicemachine.utils.ByteDataOutput;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLDate;
import org.apache.derby.iapi.types.SQLTime;
import org.apache.derby.iapi.types.SQLTimestamp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.types.DataValueFactoryImpl.Format;

public class DerbyBytesUtil extends BaseDerbyBytesUtil{
	private static Logger LOG = Logger.getLogger(DerbyBytesUtil.class);


public class DerbyBytesUtil {
		private static Logger LOG = Logger.getLogger(DerbyBytesUtil.class);

		private static final Serializer lazySerializer = new AbstractSerializer() {
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
            /*
             * We can safely setRawBytes() here because the LazyDataValueDescriptor will do it's own encoding (potentially
             * just copying values out from a KeyValue).
             */
						LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor)dvd;
						ldvd.serializeIfNeeded(desc);
						encoder.setRawBytes(ldvd.getRawBytes(),ldvd.getByteOffset(),ldvd.getByteLength());
				}

				@Override
				public void decodeInto(DataValueDescriptor dvd, MultiFieldDecoder decoder, boolean desc) throws StandardException {
						LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor)dvd;

						byte[] bytes = decoder.array();
						int offset = decoder.offset();
						skipField(decoder, dvd);

						int length = decoder.offset()-offset-1;
						ldvd.initForDeserialization(bytes, offset,length,desc);
				}

				@Override
				public boolean isScalarType() {
						throw new UnsupportedOperationException("Unable to get length from Lazy serializer");
				}

				@Override
				public void decode(DataValueDescriptor dvd, byte[] data, int offset, int length) throws StandardException {
						LazyDataValueDescriptor ldvd = (LazyDataValueDescriptor)dvd;
						ldvd.initForDeserialization(data,offset,length,false);
				}
		};

		@SuppressWarnings("unchecked")
		public static <T> T fromBytes(byte[] bytes) throws StandardException {
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


		public static byte[] generateIndexKey(DataValueDescriptor[] descriptors, boolean[] sortOrder,String tableVersion) throws IOException, StandardException {
				MultiFieldEncoder encoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),descriptors.length);
				DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion,false).getSerializers(descriptors);
				DescriptorSerializer rowLocSerializer = VersionedSerializers.forVersion(tableVersion,false).getSerializer(StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID);
				try {
        /*
         * The last entry is a RowLocation (for indices). They must be sortable, but the default encoding
         * for RowLocations is unsorted. Thus, we have to be careful to encode any RowLocation values differently
         */
						for(int i=0;i<descriptors.length;i++){
								DataValueDescriptor dvd = descriptors[i];
								boolean desc = sortOrder!=null && !sortOrder[i];
								if(dvd.getTypeFormatId()==StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID){
										rowLocSerializer.encode(encoder,dvd,desc);
//										encoder = encoder.encodeNext(dvd.getBytes(),desc);
								}else
										serializers[i].encode(encoder,dvd,desc);
//										encodeInto(encoder,dvd,desc);
						}
						return encoder.build();
				} finally {
						encoder.close();
				}
		}

		public static byte[] generateScanKeyForIndex(DataValueDescriptor[] startKeyValue,int startSearchOperator, boolean[] sortOrder,String tableVersion) throws IOException, StandardException {
				if(startKeyValue==null)return null;
				switch(startSearchOperator) { // public static final int GT = -1;
						case ScanController.NA:
						case ScanController.GE:
								return generateIndexKey(startKeyValue,sortOrder,tableVersion);
						case ScanController.GT:
								byte[] indexKey = generateIndexKey(startKeyValue,sortOrder,tableVersion);
                                /*
                                 * For a GT operation we want the next row in sorted order, and that's the row plus a
                                 * trailing 0x0 byte
                                 * The problem is sometimes we have composed keys such as:
                                 * 0xFF 0xFF 0xFF 0x00 0xEE 0xEE
                                 * 0xFF 0xFF 0xFF 0x00 0xEE 0xFF
                                 *
                                 * When we search for 0xFF 0xFF 0xFF we want both rows returned.
                                 *
                                 * In this case, the first row greater than anything of the form
                                 * 0xFF 0xFF 0xFF 0x00 0x?? 0x??
                                 *
                                 * Is 0xFF 0xFF 0xFF 0x01
                                 *
                                 * Here we append a 0x01 byte to the end of the key
                                 */
								return Bytes.add(indexKey, new byte[] {0x01});
						default:
								throw new RuntimeException("Error with Key Generation");
				}
		}

		public static void skip(MultiFieldDecoder rowDecoder, DataValueDescriptor dvd) {
				dvd.setToNull();
				skipField(rowDecoder, dvd);
		}

		public static void skipField(MultiFieldDecoder rowDecoder, DataValueDescriptor dvd) {
				if(isDoubleType(dvd))
						rowDecoder.skipDouble();
				else if(isFloatType(dvd))
						rowDecoder.skipFloat();
				else if(isScalarType(dvd, null))
						rowDecoder.skipLong();
				else
						rowDecoder.skip();
		}


		public static boolean isScalarType(DataValueDescriptor dvd, String tableVersion) {
				return dvd != null && VersionedSerializers.forVersion(tableVersion, true).getSerializer(dvd).isScalarType();
		}

		public static boolean isFloatType(DataValueDescriptor dvd){
				return dvd != null && dvd.getTypeFormatId() == StoredFormatIds.SQL_REAL_ID;
		}

		public static boolean isDoubleType(DataValueDescriptor dvd){
				return dvd != null && dvd.getTypeFormatId() == StoredFormatIds.SQL_DOUBLE_ID;
		}

		public static byte[] slice(MultiFieldDecoder fieldDecoder, int[] keyColumns, DataValueDescriptor[] rowArray) {
				int offset = fieldDecoder.offset();
				int size = skip(fieldDecoder, keyColumns, rowArray);
				//return to the original position
				fieldDecoder.seek(offset);
				return fieldDecoder.slice(size);
		}

		public static int skip(MultiFieldDecoder fieldDecoder, int[] keyColumns, DataValueDescriptor[] rowArray) {
				int size=0;
				for(int keyColumn:keyColumns){
						DataValueDescriptor dvd = rowArray[keyColumn];
						if(DerbyBytesUtil.isFloatType(dvd))
								size+=fieldDecoder.skipFloat();
						else if(DerbyBytesUtil.isDoubleType(dvd))
								size+=fieldDecoder.skipDouble();
						else
								size+=fieldDecoder.skip();
				}
				return size;
		}





}
