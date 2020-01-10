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

package com.splicemachine.utils;

import org.junit.Test;

import static org.junit.Assert.*;

public class ByteSliceTest {

    /* Test to ensure that ByteSlice set methods continue to replace buffer rather than mutating existing buffer. */
    @Test
    public void set_replacesBuffer_ratherThanUpdatingIt_evenIfNewArrayIsSmaller() {
        byte[] wrappedBytes = new byte[]{0, 1, 2, 3, 4, 5, 6, 7};
        ByteSlice byteSlice = ByteSlice.wrap(wrappedBytes);
        assertSame("array() returns reference to underlying buffer", wrappedBytes, byteSlice.array());

        byte[] bytesBytes = new byte[]{0, 1, 2, 3, 4};
        byteSlice.set(bytesBytes);
        assertSame("expected new array reference", bytesBytes, byteSlice.array());

        byte[] bytesBytes2 = new byte[]{0, 1, 2};
        byteSlice.set(bytesBytes2, 0, 3);
        assertSame("expected new array reference", bytesBytes2, byteSlice.array());
    }

    @Test
    public void set_zeroLength_zeroOffset() {
        byte[] bytes = new byte[]{0, 1, 2, 4, 5, 6, 7, 8, 9, 10};
        ByteSlice byteSlice = new ByteSlice();

        byteSlice.set(bytes, 0, 0);

        assertEquals(0, byteSlice.getByteCopy().length);
        assertEquals(0, byteSlice.length());
        assertEquals(0, byteSlice.offset());
    }

    @Test
    public void set_zeroLength_NonZeroOffset() {
        byte[] bytes = new byte[]{0, 1, 2, 4, 5, 6, 7, 8, 9, 10};
        ByteSlice byteSlice = new ByteSlice();

        byteSlice.set(bytes, 5, 0);

        assertEquals(0, byteSlice.getByteCopy().length);
        assertEquals(0, byteSlice.length());
        assertEquals(5, byteSlice.offset());
    }

    @Test
    public void getByteCopy() {
        byte[] bytes = new byte[]{0, 1, 2, 3, 4, 5};
        ByteSlice byteSlice = ByteSlice.wrap(bytes);
        byte[] copy = byteSlice.getByteCopy();
        assertNotSame("should not be the same", bytes, copy);
        assertArrayEquals("but should be equal", bytes, copy);
    }

    @Test
    public void toHexString() {
        ByteSlice byteSlice = new ByteSlice();
        byteSlice.set(new byte[]{18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0});
        assertEquals("1211100F0E0D0C0B0A09080706050403020100", byteSlice.toHexString());
    }

    @Test
    public void find() {
        byte[] sourceBytes = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};
        ByteSlice byteSlice = ByteSlice.wrap(sourceBytes);
        assertEquals(0, byteSlice.find((byte) 1, 0));
        assertEquals(1, byteSlice.find((byte) 2, 0));
        assertEquals(2, byteSlice.find((byte) 3, 0));

        sourceBytes = new byte[]{-1, -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, 120};
        byteSlice = ByteSlice.wrap(sourceBytes, 4, 10);
        assertEquals(0, byteSlice.find((byte) 0, 0));
        assertEquals(1, byteSlice.find((byte) 1, 0));
        assertEquals(2, byteSlice.find((byte) 2, 0));
        assertEquals(-1, byteSlice.find((byte) 120, 0));
    }

    @Test
    public void reverse() {
        byte[] bytes = new byte[]{0, 1, 2, 3};
        ByteSlice byteSlice = ByteSlice.wrap(bytes);
        byteSlice.reverse();
        assertSame(bytes, byteSlice.array());
        assertArrayEquals(new byte[]{-1, -2, -3, -4}, byteSlice.array());
    }

    @Test
    public void byteSliceBufferTooSmall() {
        ByteSlice.wrap(new byte[]{0}, 0, 1);

        ByteSlice.wrap(new byte[]{0}, 1, 0);

        try {
            ByteSlice.wrap(new byte[] {0}, 1, 1);
            fail("Expected too small exception");
        } catch (Throwable e) {
            assertTrue(e.getMessage().contains("is too short for"));
        }
        try {
            ByteSlice.wrap(new byte[0], 0, 1);
            fail("Expected too small exception");
        } catch (Throwable e) {
            assertTrue(e.getMessage().contains("is too short for"));
        }
        try {
            ByteSlice.wrap(new byte[0], 1, 0);
            fail("Expected too small exception");
        } catch (Throwable e) {
            assertTrue(e.getMessage().contains("is too short for"));
        }
    }
}
