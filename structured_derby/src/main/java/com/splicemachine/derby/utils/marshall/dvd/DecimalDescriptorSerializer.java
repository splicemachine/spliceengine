package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * @author Scott Fines
 * Date: 4/2/14
 */
class DecimalDescriptorSerializer implements DescriptorSerializer {
		private static final DescriptorSerializer INSTANCE = new DecimalDescriptorSerializer();
		public static final Factory INSTANCE_FACTORY = new Factory() {
				@Override
				public DescriptorSerializer newInstance() {
						return INSTANCE;
				}
				@Override public boolean applies(DataValueDescriptor dvd) { return dvd!=null && applies(dvd.getTypeFormatId()); }
				@Override public boolean applies(int typeFormatId) { return typeFormatId == StoredFormatIds.SQL_DECIMAL_ID; }

				@Override public boolean isScalar() { return false; }
				@Override public boolean isFloat() { return false; }
				@Override public boolean isDouble() { return false; }
		};

		private DecimalDescriptorSerializer() { }


		@Override
		public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
			fieldEncoder.encodeNext((BigDecimal)dvd.getObject(),desc);
		}

		@Override
		public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
				return Encoding.encode((BigDecimal)dvd.getObject(),desc);
		}

		@Override
		public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
				destDvd.setBigDecimal(fieldDecoder.decodeNextBigDecimal(desc));
		}

		@Override
		public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
				dvd.setBigDecimal(Encoding.decodeBigDecimal(data,offset,length,desc));
		}

		@Override public boolean isScalarType() { return false; }
		@Override public boolean isFloatType() { return false; }
		@Override public boolean isDoubleType() { return false; }
		@Override public void close() throws IOException {  }
}
