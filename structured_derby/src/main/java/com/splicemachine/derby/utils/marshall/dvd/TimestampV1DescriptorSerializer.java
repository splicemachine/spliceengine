package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.sql.Timestamp;

/**
 * Encodes timestamps according to the v1 format(truncating to milliseconds precision).
 *
 * @author Scott Fines
 * Date: 4/2/14
 */
class TimestampV1DescriptorSerializer extends AbstractTimeDescriptorSerializer {
		public static final Factory INSTANCE_FACTORY = new AbstractTimeDescriptorSerializer.Factory() {
				@Override public DescriptorSerializer newInstance() { return new TimestampV1DescriptorSerializer(); }
				@Override public boolean applies(int typeFormatId) { return typeFormatId == StoredFormatIds.SQL_TIMESTAMP_ID; }
		};

		protected TimestampV1DescriptorSerializer() { }


		@Override
		public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
			fieldEncoder.encodeNext(toLong(dvd.getTimestamp(getCalendar())),desc);
		}

		@Override
		public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
				return Encoding.encode(toLong(dvd.getTimestamp(getCalendar())), desc);
		}

		@Override
		public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
				long time = fieldDecoder.decodeNextLong(desc);
				destDvd.setValue(toTimestamp(time));
		}

		@Override
		public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
				dvd.setValue(toTimestamp(Encoding.decodeLong(data, offset, desc)));
		}

		protected long toLong(Timestamp timestamp) throws StandardException {
				return timestamp.getTime();
		}

		protected Timestamp toTimestamp(long time) {
				return new Timestamp(time);
		}
}
