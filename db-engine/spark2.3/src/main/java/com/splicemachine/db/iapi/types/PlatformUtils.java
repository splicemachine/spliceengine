package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeArrayWriter;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.Platform;
/**
 * Created by jyuan on 11/14/19.
 */
public class PlatformUtils {

    public static void writeSQLArray(UnsafeRowWriter unsafeRowWriter,
                                     int ordinal, DataValueDescriptor[] value) throws StandardException {
        UnsafeArrayWriter unsafeArrayWriter = new UnsafeArrayWriter();
        BufferHolder bh = new BufferHolder(new UnsafeRow(value.length));
        unsafeArrayWriter.initialize(bh, value.length, 8); // 4 bytes for int?
        for (int i = 0; i< value.length; i++) {
            if (value[i] == null || value[i].isNull()) {
                unsafeArrayWriter.setNull(i);
            } else {
                value[i].writeArray(unsafeArrayWriter, i);
            }
        }
        long currentOffset = unsafeRowWriter.holder().cursor;
        unsafeRowWriter.setOffsetAndSize(ordinal, bh.cursor-16);
        unsafeRowWriter.holder().grow(bh.cursor-16);
        Platform.copyMemory(bh.buffer,16,unsafeRowWriter.holder().buffer,currentOffset,bh.cursor-16);
    }

    public static void writeTimestamp(UnsafeRowWriter unsafeRowWriter, int ordinal,
                                      int encodedDate, int encodedTime, int nanos) {
        BufferHolder holder = unsafeRowWriter.holder();
        holder.grow(12);
        Platform.putInt(holder.buffer, holder.cursor, encodedDate);
        Platform.putInt(holder.buffer, holder.cursor + 4, encodedTime);
        Platform.putInt(holder.buffer, holder.cursor + 8, nanos);
        unsafeRowWriter.setOffsetAndSize(ordinal, 12);
        holder.cursor += 12;
    }
}
