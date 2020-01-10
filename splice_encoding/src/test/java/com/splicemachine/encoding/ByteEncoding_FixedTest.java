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

package com.splicemachine.encoding;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * @author Scott Fines
 *         Created on: 10/3/13
 */
public class ByteEncoding_FixedTest {

    @Test
    public void testCanEncodeAndDecodeValueCorrectlyDescending() throws Exception {
        /*
         * Regression test for specific, found erronous value
         */

        byte[] data = new byte[]{-116, 34, -81, 77, 103, -57};

        byte[] encoded = ByteEncoding.encode(data, true);
        byte[] decoded = ByteEncoding.decode(encoded, true);

        Assert.assertArrayEquals("Incorrect encode/decode!", data, decoded);

    }

    @Test
    public void encodeUnsorted_decodeUnsorted() {
        byte[] originalBytes = new byte[]{
                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
                17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32};

        byte[] encoded = ByteEncoding.encodeUnsorted(originalBytes);
        byte[] decoded = ByteEncoding.decodeUnsorted(encoded, 0, encoded.length);

        assertArrayEquals(originalBytes, decoded);
    }


    @Test
    public void encodeUnsorted() {
        final BitFormat BF = new BitFormat();

        byte[] bytesIn = new byte[]{1};
        byte[] bytesOut = ByteEncoding.encodeUnsorted(bytesIn);
        assertEquals("00000001", BF.format(bytesIn));
        assertEquals("10000000 11000000", BF.format(bytesOut));

        bytesIn = new byte[]{1, 2};
        bytesOut = ByteEncoding.encodeUnsorted(bytesIn);
        assertEquals("00000001 00000010", BF.format(bytesIn));
        assertEquals("10000000 11000000 11000000", BF.format(bytesOut));

        bytesIn = new byte[]{1, 2, 3};
        bytesOut = ByteEncoding.encodeUnsorted(bytesIn);
        assertEquals("00000001 00000010 00000011", BF.format(bytesIn));
        assertEquals("10000000 11000000 11000000 10110000", BF.format(bytesOut));

        bytesIn = new byte[]{-1, -1, -1};
        bytesOut = ByteEncoding.encodeUnsorted(bytesIn);
        assertEquals("11111111 11111111 11111111", BF.format(bytesIn));
        assertEquals("11111111 11111111 11111111 11110000", BF.format(bytesOut));
    }

}
