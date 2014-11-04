package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 4/2/14
 */
public class ScalarDescriptorSerializer implements DescriptorSerializer{
		static final DescriptorSerializer INSTANCE = new ScalarDescriptorSerializer();
		public static final Factory INSTANCE_FACTORY = new Factory() {
				@Override public DescriptorSerializer newInstance() { return INSTANCE; }

				@Override public boolean applies(DataValueDescriptor dvd) { return dvd == null || applies(dvd.getTypeFormatId()); }

				@Override
				public boolean applies(int typeFormatId) {
						switch(typeFormatId){
								case StoredFormatIds.SQL_TINYINT_ID:
								case StoredFormatIds.SQL_SMALLINT_ID:
								case StoredFormatIds.SQL_INTEGER_ID:
								case StoredFormatIds.SQL_LONGINT_ID:
										return true;
								default:
										return false;
						}
				}

				@Override public boolean isScalar() { return true; }
				@Override public boolean isFloat() { return false; }
				@Override public boolean isDouble() { return false; }
		};

		private ScalarDescriptorSerializer(){}


		@Override
		public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
				switch(dvd.getTypeFormatId()){
						case StoredFormatIds.SQL_TINYINT_ID:
								fieldEncoder.encodeNext(dvd.getByte(),desc); return;
						case StoredFormatIds.SQL_SMALLINT_ID:
								fieldEncoder.encodeNext(dvd.getShort(),desc); return;
						case StoredFormatIds.SQL_INTEGER_ID:
								fieldEncoder.encodeNext(dvd.getInt(),desc); return;
						case StoredFormatIds.SQL_LONGINT_ID:
								fieldEncoder.encodeNext(dvd.getLong(),desc); return;
						default:
								throw new IllegalArgumentException("Attempted to encode a value that does not have a scalar type format id");
				}
		}

		@Override
		public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
				switch(dvd.getTypeFormatId()){
						case StoredFormatIds.SQL_TINYINT_ID:
								return Encoding.encode(dvd.getByte(),desc);
						case StoredFormatIds.SQL_SMALLINT_ID:
								return Encoding.encode(dvd.getShort(),desc);
						case StoredFormatIds.SQL_INTEGER_ID:
								return Encoding.encode(dvd.getInt(),desc);
						case StoredFormatIds.SQL_LONGINT_ID:
								return Encoding.encode(dvd.getLong(),desc);
						default:
								throw new IllegalArgumentException("Attempted to encode a value that does not have a scalar type format id");
				}
		}

		@Override
		public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
				switch(destDvd.getTypeFormatId()){
						case StoredFormatIds.SQL_TINYINT_ID:
								destDvd.setValue(fieldDecoder.decodeNextByte(desc)); return;
						case StoredFormatIds.SQL_SMALLINT_ID:
								destDvd.setValue(fieldDecoder.decodeNextShort(desc)); return;
						case StoredFormatIds.SQL_INTEGER_ID:
								destDvd.setValue(fieldDecoder.decodeNextInt(desc)); return;
						case StoredFormatIds.SQL_LONGINT_ID:
								destDvd.setValue(fieldDecoder.decodeNextLong(desc)); return;
						default:
								throw new IllegalArgumentException("Attempted to decode into a descriptor which does not have a scalar type format id");
				}
		}

		@Override
		public void decodeDirect(DataValueDescriptor destDvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
				switch(destDvd.getTypeFormatId()){
						case StoredFormatIds.SQL_TINYINT_ID:
								destDvd.setValue(Encoding.decodeByte(data, offset, desc));return;
						case StoredFormatIds.SQL_SMALLINT_ID:
								destDvd.setValue(Encoding.decodeShort(data,offset,desc)); return;
						case StoredFormatIds.SQL_INTEGER_ID:
								destDvd.setValue(Encoding.decodeInt(data,offset,desc)); return;
						case StoredFormatIds.SQL_LONGINT_ID:
								destDvd.setValue(Encoding.decodeLong(data,offset,desc)); return;
						default:
								throw new IllegalArgumentException("Attempted to decode into a descriptor which does not have a scalar type format id");
				}
		}

		@Override public boolean isScalarType() { return true; }
		@Override public boolean isFloatType() { return false; }
		@Override public boolean isDoubleType() { return false; }

		@Override public void close() throws IOException {  }
}
