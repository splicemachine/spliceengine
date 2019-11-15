package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.Platform;

/**
 * Created by jyuan on 11/14/19.
 */
public class PlatformUtils {

    public static void writeSQLArray(UnsafeRowWriter unsafeRowWriter, int ordinal, DataValueDescriptor[] value) throws StandardException {
        int oldCursor = unsafeRowWriter.cursor();
        int elementSize = 8;
        for (int i = 0; i < value.length; i++)
            if (value[i] != null) {
                elementSize = getUnsafeArrayElementSize(value[i]);
                break;
            }
        UnsafeArrayWriter unsafeArrayWriter = new UnsafeArrayWriter(unsafeRowWriter, elementSize);
        unsafeArrayWriter.initialize(value.length);
        for (int i = 0; i< value.length; i++) {
            if (value[i] == null || value[i].isNull()) {
                unsafeArrayWriter.setNull(i);
            } else {
                value[i].writeArray(unsafeArrayWriter, i);
            }
        }
        unsafeRowWriter.setOffsetAndSizeFromPreviousCursor(ordinal, oldCursor);
    }

    public static void writeTimestamp(UnsafeRowWriter unsafeRowWriter, int ordinal,
                                      int encodedDate, int encodedTime, int nanos) {
        // Row size should be a multiple of 8.
        unsafeRowWriter.grow(16);
        Platform.putInt(unsafeRowWriter.getBuffer(), unsafeRowWriter.cursor(), encodedDate);
        Platform.putInt(unsafeRowWriter.getBuffer(), unsafeRowWriter.cursor() + 4, encodedTime);
        Platform.putInt(unsafeRowWriter.getBuffer(), unsafeRowWriter.cursor() + 8, nanos);
        int previousCursor = unsafeRowWriter.cursor();
        unsafeRowWriter.increaseCursor(16);
        unsafeRowWriter.setOffsetAndSizeFromPreviousCursor(ordinal, previousCursor);
    }

    private static int getUnsafeArrayElementSize (DataValueDescriptor dvd) {

        if (dvd instanceof SQLBinary) {
            return 8;
        }
        else if (dvd instanceof SQLBoolean) {
            return 1;
        }
        else if (dvd instanceof SQLChar) {
            return 8;
        }
        else if (dvd instanceof SQLDate) {
            return 4;
        }
        else if (dvd instanceof SQLDecimal) {
            return ((SQLDecimal) dvd).getPrecision() <= org.apache.spark.sql.types.Decimal.MAX_LONG_DIGITS() ? 8 : 16;
        }
        else if (dvd instanceof SQLDouble) {
            return 8;
        }
        else if (dvd instanceof SQLInteger) {
            return 4;
        }
        else if (dvd instanceof SQLLongint) {
            return 8;
        }
        else if (dvd instanceof SQLReal) {
            return 4;
        }
        else if (dvd instanceof SQLRef) {
            return 8;
        }
        else if (dvd instanceof SQLRowId) {
            return 8;
        }
        else if (dvd instanceof SQLSmallint) {
            return 2;
        }
        else if (dvd instanceof SQLTime) {
            return 8;
        }
        else if (dvd instanceof SQLTimestamp) {
            return 8;
        }
        else if (dvd instanceof SQLTinyint) {
            return 1;
        }
        else if (dvd instanceof UserType) {
            return 8;
        }
        else if (dvd instanceof XML) {
            return 8;
        }

        throw new RuntimeException("Unknown UnsafeArrayElementSize!");
    }
}
