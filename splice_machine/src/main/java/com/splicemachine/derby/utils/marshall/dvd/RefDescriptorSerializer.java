package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RefDataValue;
import org.apache.derby.iapi.types.RowLocation;

public class RefDescriptorSerializer implements DescriptorSerializer {
	
	// Similar to UnsortedBinaryDescriptorSerializer except our byte array
	// is within the RowLocation within the DVD, instead of directly
	// contained by the DVD.

	private static final DescriptorSerializer INSTANCE = new RefDescriptorSerializer();
	
	public static final Factory INSTANCE_FACTORY = new Factory() {
		@Override
		public DescriptorSerializer newInstance() {
			return INSTANCE;
		}

		@Override
		public boolean applies(DataValueDescriptor dvd) {
			return dvd != null && applies(dvd.getTypeFormatId());
		}

		@Override
		public boolean applies(int typeFormatId) {
			return typeFormatId == StoredFormatIds.SQL_REF_ID;
		}

		@Override public boolean isScalar() { return false; }
		@Override public boolean isFloat() { return false; }
		@Override public boolean isDouble() { return false; }
	};

	private RefDescriptorSerializer() { }

	@Override
	public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
		fieldEncoder.encodeNextUnsorted(((RowLocation)dvd.getObject()).getBytes());
	}

	@Override
	public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
		return Encoding.encodeBytesUnsorted(((RowLocation)dvd.getObject()).getBytes());
	}

	@Override
	public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
		byte[] bytes = fieldDecoder.decodeNextBytesUnsorted();
		((RefDataValue)destDvd).setValue(new HBaseRowLocation(bytes));
	}

	@Override
	public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
		// TODO: dvd.getObject() probably returns NULL so instantiate RowLocation like in 'decode' method
		((RowLocation)dvd.getObject()).setValue(Encoding.decodeBytesUnsortd(data,offset,length));
	}

	@Override public boolean isScalarType() { return false; }
	@Override public boolean isFloatType() { return false; }
	@Override public boolean isDoubleType() { return false; }

	@Override public void close() { }
}
