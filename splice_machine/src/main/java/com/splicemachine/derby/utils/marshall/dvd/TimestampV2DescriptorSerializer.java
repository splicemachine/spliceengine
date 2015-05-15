package com.splicemachine.derby.utils.marshall.dvd;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
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

    // edges for our internal Timestamp in microseconds
    // from ~21 Sep 1677 00:12:44 GMT to ~11 Apr 2262 23:47:16 GMT
    private static final long MAX_TIMESTAMP = Long.MAX_VALUE / MICROS_TO_SECOND - 1;
    private static final long MIN_TIMESTAMP = Long.MIN_VALUE / MICROS_TO_SECOND + 1;

    @Override
    protected long toLong(Timestamp timestamp) throws StandardException {
        return formatLong(timestamp);
    }

    @Override
    protected Timestamp toTimestamp(long time) {
        return parseTimestamp(time);
    }


    public static long formatLong(Timestamp timestamp) throws StandardException {
        long millis = timestamp.getTime();
        if (millis > MAX_TIMESTAMP || millis < MIN_TIMESTAMP) {
            throw StandardException.newException(SQLState.LANG_DATE_TIME_ARITHMETIC_OVERFLOW, timestamp.toString());
        }

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
