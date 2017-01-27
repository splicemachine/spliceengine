/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

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
			fieldEncoder.encodeNext(toLong(dvd.getTimestamp(null)),desc);
		}

		@Override
		public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
				return Encoding.encode(toLong(dvd.getTimestamp(null)), desc);
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
