package com.splicemachine.db.iapi.util;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.primitives.Bytes;

import java.sql.Timestamp;
import java.time.Instant;

import static com.splicemachine.uuid.Snowflake.TIMESTAMP_MASK;
import static com.splicemachine.uuid.Snowflake.TIMESTAMP_SHIFT;

public interface RowIdUtil {

    static String toHBaseEscaped(String s) throws StandardException {
        if(s == null) {
            return null;
        }
        if(s.length() % 2 != 0) {
            throw new IllegalArgumentException("argument length must be an even number");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i += 2) {
            sb.append("\\x").append(s, i, i+2);
        }
        return sb.toString();
    }

    static StringDataValue toHBaseEscaped(DataValueDescriptor v) throws StandardException {
        if(v == null) {
            return null;
        }
        try {
            return new SQLVarchar(toHBaseEscaped(v.getString()));
        } catch(IllegalArgumentException e) {
            throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARGUMENT, v, "TO_HBASE_ESCAPED");
        }
    }

    static DateTimeDataValue toInstant(DataValueDescriptor s) throws StandardException {
        if(s == null) {
            return null;
        }
        long value;
        try {
            String hex = toHBaseEscaped(s.getString());
            value = Bytes.toLong(Bytes.toBytesBinary(hex));
        } catch(IllegalArgumentException e) {
            throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARGUMENT, s, "TO_INSTANT");
        }

        long ts = System.currentTimeMillis() & (~TIMESTAMP_MASK);
        ts |= (value >> TIMESTAMP_SHIFT) & TIMESTAMP_MASK;
        return new SQLTimestamp(new Timestamp(ts));
    }
}
