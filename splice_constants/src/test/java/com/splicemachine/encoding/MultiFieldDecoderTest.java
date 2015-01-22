package com.splicemachine.encoding;

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

}
