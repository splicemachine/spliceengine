/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.primitives.Bytes;
import org.junit.Test;
import java.math.BigDecimal;
import static org.junit.Assert.*;

public class MultiFieldDecoderTest {

    private MultiFieldDecoder decoder = MultiFieldDecoder.create();

    @Test
    public void testDecodeMultipleContiguousNonNull() throws Exception {

        MultiFieldEncoder encoder = MultiFieldEncoder.create(4)
                .encodeNext("A")
                .encodeNext("B")
                .encodeNext("C")
                .encodeNext("D");

        byte[] encodedBytes = encoder.build();
        decoder.set(encodedBytes);

        assertEquals("A", decoder.decodeNextString());
        assertEquals("B", decoder.decodeNextString());
        assertEquals("C", decoder.decodeNextString());
        assertEquals("D", decoder.decodeNextString());

        assertArrayEquals(new byte[]{67, 0, 68, 0, 69, 0, 70}, encodedBytes);
    }

    @Test
    public void testContiguousNull() throws Exception {

        MultiFieldEncoder encoder = MultiFieldEncoder.create(6)
                .encodeNext("A")
                .encodeEmpty()
                .encodeNext("B")
                .encodeEmpty()
                .encodeEmpty()
                .encodeNext("C");

        byte[] encodedBytes = encoder.build();

        decoder.set(encodedBytes);

        assertEquals("A", decoder.decodeNextString());
        assertEquals(0, decoder.decodeNextBytes().length);
        assertEquals("B", decoder.decodeNextString());
        assertEquals(0, decoder.decodeNextBytes().length);
        assertEquals(0, decoder.decodeNextBytes().length);
        assertEquals("C", decoder.decodeNextString());

        assertArrayEquals(new byte[]{67, 0, 0, 68, 0, 0, 0, 69}, encodedBytes);
    }

    @Test
    public void testNonContiguousNull() throws Exception {

        MultiFieldEncoder encoder = MultiFieldEncoder.create(6)
                .encodeNext("A")
                .encodeEmpty()
                .encodeNext("B")
                .encodeEmpty()
                .encodeNext("C")
                .encodeEmpty();

        byte[] encodedBytes = encoder.build();

        decoder.set(encodedBytes);

        assertEquals("A", decoder.decodeNextString());
        assertEquals(0, decoder.decodeNextBytes().length);
        assertEquals("B", decoder.decodeNextString());
        assertEquals(0, decoder.decodeNextBytes().length);
        assertEquals("C", decoder.decodeNextString());
        assertEquals(0, decoder.decodeNextBytes().length);

        assertArrayEquals(new byte[]{67, 0, 0, 68, 0, 0, 69, 0}, encodedBytes);
    }

    @Test
    public void testContiguousLeadingNull() throws Exception {

        MultiFieldEncoder encoder = MultiFieldEncoder.create(6)
                .encodeEmpty()
                .encodeEmpty()
                .encodeNext("B")
                .encodeEmpty()
                .encodeNext("C")
                .encodeEmpty();

        byte[] encodedBytes = encoder.build();

        decoder.set(encodedBytes);

        assertEquals(0, decoder.decodeNextBytes().length);
        assertEquals(0, decoder.decodeNextBytes().length);
        assertEquals("B", decoder.decodeNextString());
        assertEquals(0, decoder.decodeNextBytes().length);
        assertEquals("C", decoder.decodeNextString());
        assertEquals(0, decoder.decodeNextBytes().length);

        assertArrayEquals(new byte[]{0, 0, 68, 0, 0, 69, 0}, encodedBytes);
    }

    @Test
    public void testContiguousTrailingNull() throws Exception {

        MultiFieldEncoder encoder = MultiFieldEncoder.create(6)
                .encodeEmpty()
                .encodeEmpty()
                .encodeEmpty()
                .encodeNext("C")
                .encodeEmpty()
                .encodeEmpty();

        byte[] encodedBytes = encoder.build();

        decoder.set(encodedBytes);

        assertEquals(0, decoder.decodeNextBytes().length);
        assertEquals(0, decoder.decodeNextBytes().length);
        assertEquals(0, decoder.decodeNextBytes().length);
        assertEquals("C", decoder.decodeNextString());
        assertEquals(0, decoder.decodeNextBytes().length);
        assertEquals(0, decoder.decodeNextBytes().length);

        assertArrayEquals(new byte[]{0, 0, 0, 69, 0, 0}, encodedBytes);
    }

    @Test
    public void testAllNull() throws Exception {

        MultiFieldEncoder encoder = MultiFieldEncoder.create(6)
                .encodeEmpty()
                .encodeEmpty()
                .encodeEmpty()
                .encodeEmpty()
                .encodeEmpty()
                .encodeEmpty();

        byte[] encodedBytes = encoder.build();

        decoder.set(encodedBytes);

        assertTrue(decoder.nextIsNull());
        assertEquals(0, decoder.decodeNextBytes().length);
        assertTrue(decoder.nextIsNull());
        assertEquals(0, decoder.decodeNextBytes().length);
        assertTrue(decoder.nextIsNull());
        assertEquals(0, decoder.decodeNextBytes().length);
        assertTrue(decoder.nextIsNull());
        assertEquals(0, decoder.decodeNextBytes().length);
        assertTrue(decoder.nextIsNull());
        assertEquals(0, decoder.decodeNextBytes().length);
        assertTrue(decoder.nextIsNull());
        assertEquals(0, decoder.decodeNextBytes().length);

        assertArrayEquals(new byte[]{0, 0, 0, 0, 0}, encodedBytes);
    }


    @Test
    public void testCanDecodeZeroBigDecimalFollowedByNumbers() throws Exception {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(3);
        encoder.encodeNext(BigDecimal.ZERO);
        encoder.encodeNext("c_credit1");
        encoder.encodeNext("c");

        MultiFieldDecoder decoder = MultiFieldDecoder.create();
        decoder.set(encoder.build());

        BigDecimal val = decoder.decodeNextBigDecimal();
        String next = decoder.decodeNextString();
        String last = decoder.decodeNextString();

        assertTrue("Incorrect BigDecimal!", val.compareTo(BigDecimal.ZERO) == 0);
        assertEquals("Incorrect second column!", "c_credit1", next);
        assertEquals("Incorrect third column!", "c", last);
    }

    @Test
    public void testCanDecodeBigDecimalInMiddle() throws Exception {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(3);
        encoder.encodeNext("hello");
        encoder.encodeNext(BigDecimal.valueOf(25));
        encoder.encodeNext("goodbye");

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(encoder.build());
        assertEquals("hello", decoder.decodeNextString());
        assertEquals(BigDecimal.valueOf(25), decoder.decodeNextBigDecimal());
        assertEquals("goodbye", decoder.decodeNextString());
    }

    @Test
    public void testCanDecodeSpecialDoubleInMiddleOfTwoStrings() throws Exception {
        double v = 5.5d;
        MultiFieldEncoder encoder = MultiFieldEncoder.create(3);
        encoder.encodeNext("hello").encodeNext(v).encodeNext("goodbye");
        byte[] build = encoder.build();

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(build);
        String hello = decoder.decodeNextString();
        double v1 = decoder.decodeNextDouble();
        String goodbye = decoder.decodeNextString();

        assertEquals("hello", hello);
        assertEquals(v, v1, Math.pow(10, -6));
        assertEquals("goodbye", goodbye);
    }

    @Test
    public void testThat_skipLong_skipFloat_skipDouble_returnCorrectOffsetDelta() {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(7);
        encoder.encodeNext("a");
        encoder.encodeNext(123_456_789_012L); // encoded as 6 bytes
        encoder.encodeNext(1L);               // encoded as 1 byte
        encoder.encodeNext("a");
        encoder.encodeNext(3.14159f);
        encoder.encodeNext("a");
        encoder.encodeNext(3.14159d);
        byte[] bytes = encoder.build();

        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(bytes);
        assertEquals(2, decoder.skip());
        assertEquals(7, decoder.skipLong());
        assertEquals(2, decoder.skipLong());
        assertEquals(2, decoder.skip());
        assertEquals(5, decoder.skipFloat());
        assertEquals(2, decoder.skip());
        assertEquals(9, decoder.skipDouble());
    }

    @Test
    public void nullEncodedIndex() {
        // create table T(a varchar(9), b varchar(9), c varchar(9), d varchar(9) unique, primary key(a,b,c));
        // insert into T values('E', 'E', 'E', null);
        // scan of INDEX for t shows: \x00\xA3\xC0\x88\xF0\x82\x9C    column=V:7, timestamp=3239, value=\x84\x00\xA3\xC0\x88\xF0\x82\x9C

        String hexIndexRowKeySuffix = "\\xA3\\xC0\\x88\\xF0\\x82\\x9C".replace("\\", "").replaceAll("x", "");
        byte[] indexRowKeySuffixBytes = Bytes.fromHex(hexIndexRowKeySuffix);
        byte[] baseTableRowKeyBytes = ByteEncoding.decodeUnsorted(indexRowKeySuffixBytes, 0, indexRowKeySuffixBytes.length);

        MultiFieldDecoder d = MultiFieldDecoder.create();
        d.set(baseTableRowKeyBytes);
        assertEquals("E", d.decodeNextString());
        assertEquals("E", d.decodeNextString());
        assertEquals("E", d.decodeNextString());
    }
}
