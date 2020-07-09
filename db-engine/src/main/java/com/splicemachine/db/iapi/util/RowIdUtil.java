package com.splicemachine.db.iapi.util;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.primitives.Bytes;

import java.sql.Timestamp;
import java.time.Instant;

import static com.splicemachine.uuid.Snowflake.TIMESTAMP_MASK;
import static com.splicemachine.uuid.Snowflake.TIMESTAMP_SHIFT;

public interface RowIdUtil {

    static String toHBaseEscaped(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i += 2) {
            sb.append("\\x").append(s, i, i+2);
        }
        return sb.toString();
    }

    static DateTimeDataValue toInstant(DataValueDescriptor s) throws StandardException {
        if(s == null) {
            return null;
        }
        String hex = toHBaseEscaped(s.getString());
        long value = Bytes.toLong(Bytes.toBytesBinary(hex));
        long ts = System.currentTimeMillis() & (~TIMESTAMP_MASK);
        ts |= (value >> TIMESTAMP_SHIFT) & TIMESTAMP_MASK;
        return new SQLTimestamp(new Timestamp(ts));
    }
}
