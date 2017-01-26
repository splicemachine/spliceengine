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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.db.shared.common.reference.SQLState;

import java.sql.Timestamp;

/**
 * @author Scott Fines
 *         Date: 4/2/14
 */
public class TimestampV2DescriptorSerializer extends TimestampV1DescriptorSerializer {
    public static final Factory INSTANCE_FACTORY = new AbstractTimeDescriptorSerializer.Factory() {
        @Override
        public DescriptorSerializer newInstance() {
            return new TimestampV2DescriptorSerializer();
        }

        @Override
        public boolean applies(int typeFormatId) {
            return typeFormatId == StoredFormatIds.SQL_TIMESTAMP_ID;
        }
    };

    private static final int MICROS_TO_SECOND = 1000000;
    private static final int NANOS_TO_MICROS = 1000;

    @Override
    protected long toLong(Timestamp timestamp) throws StandardException {
        return formatLong(timestamp);
    }

    @Override
    protected Timestamp toTimestamp(long time) {
        return parseTimestamp(time);
    }


    public static long formatLong(Timestamp timestamp) throws StandardException {
        long millis = SQLTimestamp.checkBounds(timestamp);
        long micros = timestamp.getNanos() / NANOS_TO_MICROS;
        return millis * MICROS_TO_SECOND + micros;
    }

    public static Timestamp parseTimestamp(long time) {
        int micros = (int) (time % MICROS_TO_SECOND);
        long millis;
        if (micros < 0) {
            micros = MICROS_TO_SECOND + micros;
            time -= micros;
        }

        millis = time / MICROS_TO_SECOND;

        Timestamp ts = new Timestamp(millis);
        ts.setNanos(micros * NANOS_TO_MICROS);
        return ts;
    }
}
