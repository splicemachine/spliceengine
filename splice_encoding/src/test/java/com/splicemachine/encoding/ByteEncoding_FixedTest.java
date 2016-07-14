/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
