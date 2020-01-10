/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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


import org.junit.Test;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import static com.splicemachine.orc.stream.TestingBitPackingUtils.unpackGeneric;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class TestLongBitPacker
{
    public static final int LENGTHS = 128;
    public static final int OFFSETS = 4;
    public static final int WIDTHS = 64;

    @Test
    public void testBasic()
            throws Throwable
    {
        LongBitPacker packer = new LongBitPacker();
        for (int length = 0; length < LENGTHS; length++) {
            assertUnpacking(packer, length);
        }
    }

    private static void assertUnpacking(LongBitPacker packer, int length)
            throws IOException
    {
        for (int width = 1; width <= WIDTHS; width++) {
            for (int offset = 0; offset < OFFSETS; offset++) {
                long[] expected = new long[length + offset];
                long[] actual = new long[length + offset];
                RandomByteInputStream expectedInput = new RandomByteInputStream();
                unpackGeneric(expected, offset, length, width, expectedInput);
                RandomByteInputStream actualInput = new RandomByteInputStream();
                packer.unpack(actual, offset, length, width, actualInput);
                for (int i = offset; i < length + offset; i++) {
                    assertEquals(format("index = %s, length = %s, width = %s, offset = %s", i, length, width, offset),actual[i], expected[i]);
                }
                assertEquals(format("Wrong number of bytes read for length = %s, width = %s, offset = %s", length, width, offset),actualInput.getReadBytes(), expectedInput.getReadBytes());
            }
        }
    }

    private static final class RandomByteInputStream
            extends InputStream
    {
        private final Random rand = new Random(0);
        private int readBytes;

        @Override
        public int read()
                throws IOException
        {
            readBytes++;
            return rand.nextInt(256);
        }

        public int getReadBytes()
        {
            return readBytes;
        }
    }
}
