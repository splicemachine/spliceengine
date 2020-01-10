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
 */package com.splicemachine.orc.stream;

import com.splicemachine.orc.memory.AggregatedMemoryContext;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slices;
import org.junit.Test;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import static com.splicemachine.orc.metadata.CompressionKind.UNCOMPRESSED;
import static java.math.BigInteger.ONE;
import static org.junit.Assert.assertEquals;

public class TestDecimalStream
{
    public static final int MAX_PRECISION = 38;
    public static final int MAX_SHORT_PRECISION = 18;

    public static final BigInteger MAX_DECIMAL_UNSCALED_VALUE = new BigInteger(
            // repeat digit '9' MAX_PRECISION times
            new String(new char[MAX_PRECISION]).replace("\0", "9"));
    public static final BigInteger MIN_DECIMAL_UNSCALED_VALUE = MAX_DECIMAL_UNSCALED_VALUE.negate();

    @Test
    public void testShortDecimals()
            throws IOException
    {
        assertReadsShortValue(0L);
        assertReadsShortValue(1L);
        assertReadsShortValue(-1L);
        assertReadsShortValue(256L);
        assertReadsShortValue(-256L);
        assertReadsShortValue(Long.MAX_VALUE);
        assertReadsShortValue(Long.MIN_VALUE);
    }

    @Test
    public void testShouldFailWhenShortDecimalDoesNotFit()
            throws IOException
    {
        assertShortValueReadFails(BigInteger.valueOf(Long.MAX_VALUE).add(ONE));
    }

    @Test
    public void testShouldFailWhenExceeds128Bits()
            throws IOException
    {
        assertLongValueReadFails(BigInteger.valueOf(1).shiftLeft(127));
        assertLongValueReadFails(BigInteger.valueOf(-2).shiftLeft(127));
    }

    @Test
    public void testLongDecimals()
            throws IOException
    {
        assertReadsLongValue(BigInteger.valueOf(0L));
        assertReadsLongValue(BigInteger.valueOf(1L));
        assertReadsLongValue(BigInteger.valueOf(-1L));
        assertReadsLongValue(BigInteger.valueOf(-1).shiftLeft(127));
        assertReadsLongValue(BigInteger.valueOf(1).shiftLeft(126));
        assertReadsLongValue(MAX_DECIMAL_UNSCALED_VALUE);
        assertReadsLongValue(MIN_DECIMAL_UNSCALED_VALUE);
    }

    @Test
    public void testSkipsValue()
            throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writeBigInteger(baos, BigInteger.valueOf(Long.MAX_VALUE));
        writeBigInteger(baos, BigInteger.valueOf(Long.MIN_VALUE));

        OrcInputStream inputStream = orcInputStreamFor("skip test", baos.toByteArray());
        DecimalStream stream = new DecimalStream(inputStream);
        stream.skip(1);
        assertEquals(stream.nextLong(), Long.MIN_VALUE);
    }

    private static void assertReadsShortValue(long value)
            throws IOException
    {
        DecimalStream stream = new DecimalStream(decimalInputStream(BigInteger.valueOf(value)));
        assertEquals(stream.nextLong(), value);
    }

    private static void assertReadsLongValue(BigInteger value)
            throws IOException
    {
        DecimalStream stream = new DecimalStream(decimalInputStream(value));
        assertEquals(stream.nextBigInteger(), value);
    }

    private static void assertShortValueReadFails(BigInteger value)
            throws IOException
    {
/*        assertThrows(OrcCorruptionException.class, () -> {
            DecimalStream stream = new DecimalStream(decimalInputStream(value));
            stream.nextLong();
        });
        */ // JL-TODO
    }

    private static void assertLongValueReadFails(BigInteger value)
            throws IOException
    {
        /*
        assertThrows(OrcCorruptionException.class, () -> {
            DecimalStream stream = new DecimalStream(decimalInputStream(value));
            stream.nextBigInteger();
        });
        */ // JL-TODO
    }

    private static OrcInputStream decimalInputStream(BigInteger value)
            throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writeBigInteger(baos, value);
        return orcInputStreamFor(value.toString(), baos.toByteArray());
    }

    private static OrcInputStream orcInputStreamFor(String source, byte[] bytes)
    {
        return new OrcInputStream(source, new BasicSliceInput(Slices.wrappedBuffer(bytes)), UNCOMPRESSED, 10000, new AggregatedMemoryContext());
    }

    // copied from org.apache.hadoop.hive.ql.io.orc.SerializationUtils.java
    private static void writeBigInteger(OutputStream output, BigInteger value)
            throws IOException
    {
        // encode the signed number as a positive integer
        value = value.shiftLeft(1);
        int sign = value.signum();
        if (sign < 0) {
            value = value.negate();
            value = value.subtract(ONE);
        }
        int length = value.bitLength();
        while (true) {
            long lowBits = value.longValue() & 0x7fffffffffffffffL;
            length -= 63;
            // write out the next 63 bits worth of data
            for (int i = 0; i < 9; ++i) {
                // if this is the last byte, leave the high bit off
                if (length <= 0 && (lowBits & ~0x7f) == 0) {
                    output.write((byte) lowBits);
                    return;
                }
                else {
                    output.write((byte) (0x80 | (lowBits & 0x7f)));
                    lowBits >>>= 7;
                }
            }
            value = value.shiftRight(63);
        }
    }
}
