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
class RealDescriptorSerializer implements DescriptorSerializer{
		private static final DescriptorSerializer INSTANCE = new RealDescriptorSerializer();
		public static final Factory INSTANCE_FACTORY = new Factory() {
				@Override public DescriptorSerializer newInstance() { return INSTANCE; }
				@Override public boolean applies(DataValueDescriptor dvd) { return dvd!=null && applies(dvd.getTypeFormatId()); }
				@Override public boolean applies(int typeFormatId) { return typeFormatId== StoredFormatIds.SQL_REAL_ID; }

				@Override public boolean isScalar() { return false; }
				@Override public boolean isFloat() { return true; }
				@Override public boolean isDouble() { return false; }
		};

		private RealDescriptorSerializer() { }


		@Override
		public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
				fieldEncoder.encodeNext(dvd.getFloat(),desc);
		}

		@Override
		public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
				return Encoding.encode(dvd.getFloat(), desc);
		}

		@Override
		public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
				destDvd.setValue(fieldDecoder.decodeNextFloat(desc));
		}

		@Override
		public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
				dvd.setValue(Encoding.decodeFloat(data,offset,desc));
		}

		@Override public boolean isScalarType() { return false; }
		@Override public boolean isFloatType() { return true; }
		@Override public boolean isDoubleType() { return false; }
}
