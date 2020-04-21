/*
 * Copyright (c) 2018 Splice Machine, Inc.
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


import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLDate;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;

/**
 * This class is NOT thread safe.
 *
 * @author Mark Sirek
 * Date: 9/15/18
 */
public class DateV4DescriptorSerializer extends DateDescriptorSerializer {
        public static final Factory INSTANCE_FACTORY = new Factory() {
                @Override public DescriptorSerializer newInstance() { return new DateV4DescriptorSerializer(); }
                @Override public boolean applies(int typeFormatId) { return typeFormatId == StoredFormatIds.SQL_DATE_ID; }
        };

        public static int diskEncodingToInMemEncoding(int diskEncoding)
        {
            diskEncoding += SQLDate.DATE_ENCODING_OFFSET;
            return ((diskEncoding >>> 9) << 16)           +
                   (((diskEncoding >>> 5) & 0x000f) << 8) +
                   (diskEncoding & 0x001f);
        }

        @Override
        public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
                fieldEncoder.encodeNext(((SQLDate)dvd).getDiskEncodedDate(),desc);
        }

        @Override
        public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
                return Encoding.encode(((SQLDate)dvd).getDiskEncodedDate(), desc);
        }

        @Override
        public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
                destDvd.setValue(diskEncodingToInMemEncoding(fieldDecoder.decodeNextInt(desc)));
        }

        @Override
        public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
                dvd.setValue(diskEncodingToInMemEncoding(Encoding.decodeInt(data,offset,desc)));
        }
}
