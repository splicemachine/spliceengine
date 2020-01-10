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

import com.splicemachine.orc.OrcCorruptionException;
import com.splicemachine.orc.checkpoint.DecimalStreamCheckpoint;

import java.io.IOException;
import java.math.BigInteger;

import static java.lang.Long.MAX_VALUE;

public class DecimalStream
        implements ValueStream<DecimalStreamCheckpoint>
{
    private final OrcInputStream input;

    public DecimalStream(OrcInputStream input)
    {
        this.input = input;
    }

    @Override
    public Class<? extends DecimalStreamCheckpoint> getCheckpointType()
    {
        return DecimalStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(DecimalStreamCheckpoint checkpoint)
            throws IOException
    {
        input.seekToCheckpoint(checkpoint.getInputStreamCheckpoint());
    }

    // This comes from the Apache Hive ORC code (see org.apache.hadoop.hive.ql.io.orc.SerializationUtils.java)
    public BigInteger nextBigInteger()
            throws IOException
    {
        BigInteger result = BigInteger.ZERO;
        long work = 0;
        int offset = 0;
        long b;
        do {
            b = input.read();
            if (b == -1) {
                throw new OrcCorruptionException("Reading BigInteger past EOF from " + input);
            }
            work |= (0x7f & b) << (offset % 63);
            if (offset >= 126 && (offset != 126 || work > 3)) {
                throw new OrcCorruptionException("Decimal exceeds 128 bits");
            }
            offset += 7;
            // if we've read 63 bits, roll them into the result
            if (offset == 63) {
                result = BigInteger.valueOf(work);
                work = 0;
            }
            else if (offset % 63 == 0) {
                result = result.or(BigInteger.valueOf(work).shiftLeft(offset - 63));
                work = 0;
            }
        }
        while (b >= 0x80);
        if (work != 0) {
            result = result.or(BigInteger.valueOf(work).shiftLeft((offset / 63) * 63));
        }
        // convert back to a signed number
        boolean isNegative = result.testBit(0);
        if (isNegative) {
            result = result.add(BigInteger.ONE);
            result = result.negate();
        }
        result = result.shiftRight(1);
        return result;
    }

    public void nextBigIntegerVector(int items, BigInteger[] vector)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            vector[i] = nextBigInteger();
        }
    }

    public void nextBigIntegerVector(int items, BigInteger[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = nextBigInteger();
            }
        }
    }

    public long nextLong()
            throws IOException
    {
        long result = 0;
        int offset = 0;
        long b;
        do {
            b = input.read();
            if (b == -1) {
                throw new OrcCorruptionException("Reading BigInteger past EOF from " + input);
            }
            long work = 0x7f & b;
            if (offset >= 63 && (offset != 63 || work > 1)) {
                throw new OrcCorruptionException("Decimal does not fit long (invalid table schema?)");
            }
            result |= work << offset;
            offset += 7;
        }
        while (b >= 0x80);
        boolean isNegative = (result & 0x01) != 0;
        if (isNegative) {
            result += 1;
            result = -result;
            result = result >> 1;
            result |= 0x01L << 63;
        }
        else {
            result = result >> 1;
            result &= MAX_VALUE;
        }
        return result;
    }

    public void nextLongVector(int items, long[] vector)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            vector[i] = nextLong();
        }
    }

    public void nextLongVector(int items, long[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = nextLong();
            }
        }
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        while (items-- > 0) {
            int b;
            do {
                b = input.read();
                if (b == -1) {
                    throw new OrcCorruptionException("Reading BigInteger past EOF from " + input);
                }
            }
            while (b >= 0x80);
        }
    }
}
