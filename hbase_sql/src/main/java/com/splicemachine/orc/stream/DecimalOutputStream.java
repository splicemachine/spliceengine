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

import com.splicemachine.orc.OrcOutputBuffer;
import com.splicemachine.orc.checkpoint.DecimalStreamCheckpoint;
import com.splicemachine.orc.metadata.CompressionKind;
import com.splicemachine.orc.metadata.Stream;
import com.google.common.collect.ImmutableList;
import com.splicemachine.orc.types.Decimals;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.splicemachine.orc.metadata.Stream.StreamKind.DATA;
import static com.splicemachine.orc.stream.LongDecode.writeVLong;
import static com.google.common.base.Preconditions.checkState;

/**
 * This is only for mantissa/significant of a decimal and not the exponent.
 */
public class DecimalOutputStream
        implements ValueOutputStream<DecimalStreamCheckpoint>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DecimalOutputStream.class).instanceSize();
    private final OrcOutputBuffer buffer;
    private final List<DecimalStreamCheckpoint> checkpoints = new ArrayList<>();

    private boolean closed;

    public DecimalOutputStream(CompressionKind compression, int bufferSize)
    {
        this.buffer = new OrcOutputBuffer(compression, bufferSize);
    }

    // todo rewrite without BigInteger
    // This comes from the Apache Hive ORC code
    public void writeUnscaledValue(Slice slice)
    {
        BigInteger value = Decimals.decodeUnscaledValue(slice);

        // encode the signed number as a positive integer
        value = value.shiftLeft(1);
        int sign = value.signum();
        if (sign < 0) {
            value = value.negate();
            value = value.subtract(BigInteger.ONE);
        }
        int length = value.bitLength();
        while (true) {
            long lowBits = value.longValue() & 0x7fff_ffff_ffff_ffffL;
            length -= 63;
            // write out the next 63 bits worth of data
            for (int i = 0; i < 9; ++i) {
                // if this is the last byte, leave the high bit off
                if (length <= 0 && (lowBits & ~0x7f) == 0) {
                    buffer.write((byte) lowBits);
                    return;
                }
                else {
                    buffer.write((byte) (0x80 | (lowBits & 0x7f)));
                    lowBits >>>= 7;
                }
            }
            value = value.shiftRight(63);
        }
    }

    public void writeUnscaledValue(long value)
    {
        checkState(!closed);
        writeVLong(buffer, value, true);
    }

    @Override
    public void recordCheckpoint()
    {
        checkState(!closed);
        checkpoints.add(new DecimalStreamCheckpoint(buffer.getCheckpoint()));
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public List<DecimalStreamCheckpoint> getCheckpoints()
    {
        checkState(closed);
        return ImmutableList.copyOf(checkpoints);
    }

    @Override
    public Optional<Stream> writeDataStreams(int column, SliceOutput outputStream)
    {
        checkState(closed);
        int length = buffer.writeDataTo(outputStream);
        return Optional.of(new Stream(column, DATA, length, true));
    }

    @Override
    public long getBufferedBytes()
    {
        return buffer.size();
    }

    @Override
    public long getRetainedBytes()
    {
        // NOTE: we do not include checkpoints because they should be small and it would be annoying to calculate the size
        return INSTANCE_SIZE + buffer.getRetainedSize();
    }

    @Override
    public void reset()
    {
        closed = false;
        buffer.reset();
        checkpoints.clear();
    }
}

