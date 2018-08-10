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

import java.sql.Timestamp;

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


    @Override
    protected Timestamp toTimestamp(long time) {
        long millis = time / 1000;
        // nanos start of being set to the number of microseconds.
        int nanos = (int)(time % 1000000);

        if (nanos < 0) {

            // Something like -1900000 microseconds is the same as -2 seconds + 100000 microseconds,
            // setNanos takes the number of nanoseconds greater than the previous time in seconds,
            // so we need to convert the negative offset to a positive offset.
            nanos = 1000000 + nanos;

            // For proper resolution of number of seconds, we need to always round down.
            // For positive values, dividing by 1000 is sufficient, but for negatives
            // the following adjustment is required.
            // Both "time" and "nanos" both currently have units of microseconds.
            millis = (time - nanos)/1000;
        }
        // We encode with microseconds precision, but setNanos needs nanoseconds.
        nanos *= 1000;
        Timestamp ts = new Timestamp(millis);
        ts.setNanos(nanos);

        return ts;
    }

    @Override
    protected long toLong(Timestamp timestamp) throws StandardException {
        return TimestampV3DescriptorSerializer.formatLong(timestamp);
    }

    public static long formatLong(Timestamp timestamp) throws StandardException {
        int nanos = timestamp.getNanos();
        // 1. Round milliseconds down to the nearest second, e.g. -1001 ms becomes -2000 ms.
        // 2. Shift 3 decimal places to the left, so there's a total of 6 zeroes in the rightmost digits.
        // 3. Divide nanoseconds by 1000 to produce microseconds, and add that to the final value.
        long micros = (timestamp.getTime() - (nanos / 1000000)) * 1000 + (nanos / 1000);
        return micros;
    }

}
