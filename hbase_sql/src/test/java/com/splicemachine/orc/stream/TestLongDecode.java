/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.orc.stream;

import com.splicemachine.orc.OrcDataSourceId;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

import static com.splicemachine.orc.stream.LongDecode.readVInt;
import static com.splicemachine.orc.stream.LongDecode.writeVLong;
import static org.testng.Assert.assertEquals;

public class TestLongDecode
{
    @Test
    public void testVInt()
            throws Exception
    {
        Slice slice = Slices.allocate(100);
        SliceOutput output = slice.getOutput();

        assertVIntRoundTrip(output, 0);
        assertVIntRoundTrip(output, 1);
        assertVIntRoundTrip(output, -1);
        assertVIntRoundTrip(output, Integer.MAX_VALUE);
        assertVIntRoundTrip(output, Integer.MAX_VALUE + 1L);
        assertVIntRoundTrip(output, Integer.MAX_VALUE - 1L);
        assertVIntRoundTrip(output, Integer.MIN_VALUE);
        assertVIntRoundTrip(output, Integer.MIN_VALUE + 1L);
        assertVIntRoundTrip(output, Integer.MIN_VALUE - 1L);
        assertVIntRoundTrip(output, Long.MAX_VALUE);
        assertVIntRoundTrip(output, Long.MAX_VALUE - 1);
        assertVIntRoundTrip(output, Long.MIN_VALUE + 1);

        for (int value = -100_000; value < 100_000; value++) {
            assertVIntRoundTrip(output, value);
        }
    }

    private static void assertVIntRoundTrip(SliceOutput output, long value)
            throws IOException
    {
        assertVIntRoundTrip(output, value, true);
        assertVIntRoundTrip(output, value, false);
    }

    private static void assertVIntRoundTrip(SliceOutput output, long value, boolean signed)
            throws IOException
    {
        // write using Hive's code
        output.reset();
        if (signed) {
            writeVslong(output, value);
        }
        else {
            writeVulong(output, value);
        }
        Slice hiveBytes = Slices.copyOf(output.slice());

        // write using Presto's code, and verify they are the same
        output.reset();
        writeVLong(output, value, signed);
        Slice prestoBytes = Slices.copyOf(output.slice());
        if (!prestoBytes.equals(hiveBytes)) {
            assertEquals(prestoBytes, hiveBytes);
        }

        // read using Hive's code
        if (signed) {
            long readValueOld = readVslong(hiveBytes.getInput());
            assertEquals(readValueOld, value);
        }
        else {
            long readValueOld = readVulong(hiveBytes.getInput());
            assertEquals(readValueOld, value);
        }

        // read using Presto's code
        long readValueNew = readVInt(signed, new OrcInputStream(new OrcDataSourceId("test"), hiveBytes.getInput(), Optional.empty(), new AggregatedMemoryContext()));
        assertEquals(readValueNew, value);
    }

    //
    // The following was copied from package private org.apache.hadoop.hive.ql.io.orc.SerializationUtils

    private static void writeVulong(OutputStream output, long value)
            throws IOException
    {
        while (true) {
            if ((value & ~0x7f) == 0) {
                output.write((byte) value);
                return;
            }
            else {
                output.write((byte) (0x80 | (value & 0x7f)));
                value >>>= 7;
            }
        }
    }

    private static void writeVslong(OutputStream output, long value)
            throws IOException
    {
        writeVulong(output, (value << 1) ^ (value >> 63));
    }

    private static long readVulong(InputStream in)
            throws IOException
    {
        long result = 0;
        long b;
        int offset = 0;
        do {
            b = in.read();
            if (b == -1) {
                throw new EOFException("Reading Vulong past EOF");
            }
            result |= (0x7f & b) << offset;
            offset += 7;
        } while (b >= 0x80);
        return result;
    }

    private static long readVslong(InputStream in)
            throws IOException
    {
        long result = readVulong(in);
        return (result >>> 1) ^ -(result & 1);
    }
}
