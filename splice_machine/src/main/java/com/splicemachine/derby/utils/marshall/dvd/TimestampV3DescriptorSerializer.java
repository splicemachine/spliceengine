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

import com.splicemachine.db.iapi.services.io.StoredFormatIds;

/**
 * @author Mark Sirek
 *         Date: 6/6/18
 */
public class TimestampV3DescriptorSerializer extends TimestampV2DescriptorSerializer {
    public static final Factory INSTANCE_FACTORY = new AbstractTimeDescriptorSerializer.Factory() {
        @Override
        public DescriptorSerializer newInstance() {
            return new TimestampV3DescriptorSerializer();
        }

        @Override
        public boolean applies(int typeFormatId) {
            return typeFormatId == StoredFormatIds.SQL_TIMESTAMP_ID;
        }
    };
/*
    // Lower 20 bits reserved for encoding microseconds.
    protected static final long MICROS_BITMASK = 0x00000000000FFFFF;

    @Override
    protected Timestamp toTimestamp(long time) {
        long secs = time >> 20;
        long micros = time & MICROS_BITMASK;
        long millis = secs * 1000;
        int nanos = ((int)(micros)) * 1000;

        Timestamp ts = new Timestamp(millis);
        ts.setNanos(nanos);
        return ts;
    }

    @Override
    protected long toLong(Timestamp timestamp) throws StandardException {
        return TimestampV3DescriptorSerializer.formatLong(timestamp);
    }

    public static long formatLong(Timestamp timestamp) throws StandardException {
        long millis = timestamp.getTime();
        long micros = timestamp.getNanos() / 1000;
        //long millistosubtract = nanos/1000000;
        long millistosubtract = (millis < 0) ?
        (1000-(millis % 1000)) % 1000 : (millis % 1000);
        millis -= millistosubtract;
        long secs = millis / 1000;
        long retval = (secs << 20) | micros;
        return retval;
    }
*/
    @Override
    public boolean isScalarType() { return true; }

/*

    @Override
    public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
        Timestamp ts = dvd.getTimestamp(null);
        fieldEncoder.encodeNext(ts,desc);
    }

    @Override
    public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
        Timestamp ts = dvd.getTimestamp(null);
        byte[] millis;
        byte[] nanos;
        millis = Encoding.encode(toLong(ts), desc);
        nanos = Encoding.encode(ts.getNanos(), desc);
        byte[] bytes = new byte[millis.length + nanos.length];
        System.arraycopy(millis, 0, bytes, 0, millis.length);
        System.arraycopy(nanos, 0, bytes, millis.length, nanos.length);
        byte[] finalBytes = Encoding.encode(bytes, desc);
        return finalBytes;
    }

    @Override
    public void decode(MultiFieldDecoder fieldDecoder, DataValueDescriptor destDvd, boolean desc) throws StandardException {
        byte[] bytes = fieldDecoder.decodeNextBytes(desc);
        long [] intValueLength = new long[2];
        int currentOffset = 0;
        Encoding.decodeLongWithLength(bytes,currentOffset,desc,intValueLength);
        currentOffset = (int)intValueLength[1];
        long time = intValueLength[0];
        Encoding.decodeLongWithLength(bytes,currentOffset,desc,intValueLength);
        int nanos = (int)intValueLength[0];
        destDvd.setValue(toTimestamp(time, nanos));
    }

    @Override
    public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
        byte[] bytes = Encoding.decodeBytes(data, offset, length, desc);
        long [] intValueLength = new long[2];
        int currentOffset = 0;
        Encoding.decodeLongWithLength(bytes,currentOffset,desc,intValueLength);
        currentOffset = (int)intValueLength[1];
        long time = intValueLength[0];
        Encoding.decodeLongWithLength(bytes,currentOffset,desc,intValueLength);
        int nanos = (int)intValueLength[0];

        dvd.setValue(toTimestamp(time, nanos));
    }
    msirek-temp */
}
