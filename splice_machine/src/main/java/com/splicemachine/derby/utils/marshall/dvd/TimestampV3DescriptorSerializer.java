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
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;

import java.sql.Timestamp;

/**
 * @author Mark Sirek
 *         Date: 6/6/18
 */
public class TimestampV3DescriptorSerializer extends AbstractTimeStampDescriptorSerializer {
    public static final Factory INSTANCE_FACTORY = new AbstractTimeStampDescriptorSerializer.Factory() {
        @Override
        public DescriptorSerializer newInstance() {
            return new TimestampV3DescriptorSerializer();
        }

        @Override
        public boolean applies(int typeFormatId) {
            return typeFormatId == StoredFormatIds.SQL_TIMESTAMP_ID;
        }
    };

    protected long toLong(Timestamp timestamp) throws StandardException {
        long millis = timestamp.getTime();
        long nanos = timestamp.getNanos();
        long millistosubtract = nanos/1000000;

        long secs = (millis-millistosubtract) / 1000;
        return secs;
    }

    protected Timestamp toTimestamp(long time, int nanos) {
        long millis = time;

        Timestamp ts = new Timestamp(millis);
        ts.setNanos(nanos);
        return ts;
    }

    @Override
    public void encode(MultiFieldEncoder fieldEncoder, DataValueDescriptor dvd, boolean desc) throws StandardException {
        Timestamp ts = dvd.getTimestamp(null);
        fieldEncoder.encodeNext(ts,desc);
    }

    @Override
    public byte[] encodeDirect(DataValueDescriptor dvd, boolean desc) throws StandardException {
        Timestamp ts = dvd.getTimestamp(null);
        byte[] seconds;
        byte[] micros;
        seconds = Encoding.encode(toLong(ts), desc);
        micros = Encoding.encode(ts.getNanos()/1000, desc);
        byte[] bytes = new byte[seconds.length + micros.length];
        System.arraycopy(seconds, 0, bytes, 0, seconds.length);
        System.arraycopy(micros, 0, bytes, seconds.length, micros.length);
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
        long seconds = intValueLength[0];
        Encoding.decodeLongWithLength(bytes,currentOffset,desc,intValueLength);
        int micros = (int)intValueLength[0];
        long time = seconds*1000 + micros/1000;
        destDvd.setValue(toTimestamp(time, micros*1000));
    }

    @Override
    public void decodeDirect(DataValueDescriptor dvd, byte[] data, int offset, int length, boolean desc) throws StandardException {
        byte[] bytes = Encoding.decodeBytes(data, offset, length, desc);
        long [] intValueLength = new long[2];
        int currentOffset = 0;
        Encoding.decodeLongWithLength(bytes,currentOffset,desc,intValueLength);
        currentOffset = (int)intValueLength[1];
        long seconds = intValueLength[0];
        Encoding.decodeLongWithLength(bytes,currentOffset,desc,intValueLength);
        int micros = (int)intValueLength[0];
        long time = seconds*1000 + micros/1000;
        dvd.setValue(toTimestamp(time, micros*1000));
    }

    public static long formatLong(Timestamp timestamp) throws StandardException {
        return timestamp.getTime();
    }

    public static Timestamp parseTimestamp(long time) {
        Timestamp ts = new Timestamp(time);
        return ts;
    }
    @Override
    public boolean isScalarType() { return false; }

}
