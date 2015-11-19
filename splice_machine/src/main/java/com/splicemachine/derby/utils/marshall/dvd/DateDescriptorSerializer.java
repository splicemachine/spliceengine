package com.splicemachine.derby.utils.marshall.dvd;


import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import java.sql.Date;

/**
 * This class is NOT thread safe.
 *
 * @author Scott Fines
 * Date: 4/2/14
 */
class DateDescriptorSerializer extends AbstractTimeDescriptorSerializer {
		public static final Factory INSTANCE_FACTORY = new Factory() {
				@Override public DescriptorSerializer newInstance() { return new DateDescriptorSerializer(); }

				@Override public boolean applies(int typeFormatId) { return typeFormatId == StoredFormatIds.SQL_DATE_ID; }

		};

		private DateDescriptorSerializer() { }

		@Override
		public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
				fieldEncoder.encodeNext(dvd.getDate(getCalendar()).getTime(),desc);
		}

		@Override
		public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
				return Encoding.encode(dvd.getDate(getCalendar()).getTime(), desc);
		}

		@Override
		public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
				destDvd.setValue(new Date(fieldDecoder.decodeNextLong(desc)));
		}

		@Override
		public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
				dvd.setValue(new Date(Encoding.decodeLong(data,offset,desc)));
		}
}
