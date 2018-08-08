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
        //long secs = time >> 20;
        long millis = time / 1000;
        int nanos = (int)(time % 1000000);

        if (nanos < 0) {

            nanos = 1000000 + nanos;
            millis = (time - nanos)/1000;
        }
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
        long micros = (timestamp.getTime() - (nanos / 1000000)) * 1000 + (nanos / 1000);
        return micros;
    }

    @Override
    public boolean isScalarType() { return true; }

}
