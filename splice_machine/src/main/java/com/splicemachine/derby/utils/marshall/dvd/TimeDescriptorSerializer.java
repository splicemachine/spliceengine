package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 * Date: 4/2/14
 */
class TimeDescriptorSerializer extends AbstractTimeDescriptorSerializer{
		public static final Factory INSTANCE_FACTORY = new Factory() {
				@Override public DescriptorSerializer newInstance() { return new TimeDescriptorSerializer(); }
				@Override public boolean applies(int typeFormatId) { return typeFormatId == StoredFormatIds.SQL_TIME_ID; }
		};

		private TimeDescriptorSerializer() { }

		@Override
		public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
			fieldEncoder.encodeNext(dvd.getTime(getCalendar()).getTime(),desc);
		}

		@Override
		public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
				return Encoding.encode(dvd.getTime(getCalendar()).getTime(),desc);
		}

		@Override
		public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
				long timestamp = fieldDecoder.decodeNextLong(desc);
				destDvd.setValue(new java.sql.Time(timestamp));
		}

		@Override
		public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
				long timestamp = Encoding.decodeLong(data,offset,desc);
				dvd.setValue(new java.sql.Time(timestamp));
		}
}
