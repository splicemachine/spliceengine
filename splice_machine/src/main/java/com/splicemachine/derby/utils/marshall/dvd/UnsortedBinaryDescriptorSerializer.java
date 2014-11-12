package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 * Date: 4/2/14
 */
class UnsortedBinaryDescriptorSerializer implements DescriptorSerializer{
		private static final DescriptorSerializer INSTANCE = new UnsortedBinaryDescriptorSerializer();
		public static final Factory INSTANCE_FACTORY = new Factory() {
				@Override public DescriptorSerializer newInstance() { return INSTANCE; }

				@Override
				public boolean applies(DataValueDescriptor dvd) {
						return dvd!=null && applies(dvd.getTypeFormatId());
				}

				@Override
				public boolean applies(int typeFormatId) {
						switch(typeFormatId){
								case StoredFormatIds.SQL_VARBIT_ID:
								case StoredFormatIds.SQL_LONGVARBIT_ID:
								case StoredFormatIds.SQL_BLOB_ID:
								case StoredFormatIds.SQL_BIT_ID:
								case StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID:
										return true;
								default:
										return false;
						}
				}

				@Override public boolean isScalar() { return false; }
				@Override public boolean isFloat() { return false; }
				@Override public boolean isDouble() { return false; }
		};

		private UnsortedBinaryDescriptorSerializer() { }


		@Override
		public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
				fieldEncoder.encodeNextUnsorted(dvd.getBytes());
		}

		@Override
		public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
				return Encoding.encodeBytesUnsorted(dvd.getBytes());
		}

		@Override
		public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
				byte[] bytes = fieldDecoder.decodeNextBytesUnsorted();
				destDvd.setValue(bytes);
		}

		@Override
		public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
				dvd.setValue(Encoding.decodeBytesUnsortd(data,offset,length));
		}

		@Override public boolean isScalarType() { return false; }
		@Override public boolean isFloatType() { return false; }
		@Override public boolean isDoubleType() { return false; }

		@Override public void close() { }
}
