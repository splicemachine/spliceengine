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

import com.google.common.collect.ImmutableList;
import com.splicemachine.db.shared.common.sanity.SanityManager;
import com.splicemachine.orc.OrcOutputBuffer;
import com.splicemachine.orc.checkpoint.BooleanStreamCheckpoint;
import com.splicemachine.orc.checkpoint.ByteStreamCheckpoint;
import com.splicemachine.orc.metadata.CompressionKind;
import com.splicemachine.orc.metadata.Stream;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class BooleanOutputStream
        implements ValueOutputStream<BooleanStreamCheckpoint>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BooleanOutputStream.class).instanceSize();
    private final ByteOutputStream byteOutputStream;
    private final List<Integer> checkpointBitOffsets = new ArrayList<>();

    private int bitsInData;
    private int data;
    private boolean closed;

    public BooleanOutputStream(CompressionKind compression, int bufferSize)
    {
        this(new ByteOutputStream(compression, bufferSize));
    }

    public BooleanOutputStream(OrcOutputBuffer buffer)
    {
        this(new ByteOutputStream(buffer));
    }

    public BooleanOutputStream(ByteOutputStream byteOutputStream)
    {
        this.byteOutputStream = byteOutputStream;
    }

    public void writeBoolean(boolean value)
    {
        checkState(!closed);

        if (value) {
            data |= 0x1 << (7 - bitsInData);
        }
        bitsInData++;

        if (bitsInData == 8) {
            flushData();
        }
    }

    public void writeBooleans(int count, boolean value)
    {
        checkArgument(count >= 0, "count is negative");
        if (count == 0) {
            return;
        }

        if (bitsInData != 0) {
            int bitsToWrite = Math.min(count, 8 - bitsInData);
            if (value) {
                data |= getLowBitMask(bitsToWrite);
            }

            bitsInData += bitsToWrite;
            count -= bitsToWrite;
            if (bitsInData == 8) {
                flushData();
            }
            else {
                // there were not enough bits to fill the current data
                SanityManager.ASSERT(count == 0);
                return;
            }
        }

        // at this point there should be no pending data
        SanityManager.ASSERT(bitsInData == 0);

        // write 8 bits at a time
        while (count >= 8) {
            if (value) {
                byteOutputStream.writeByte((byte) 0b1111_1111);
            }
            else {
                byteOutputStream.writeByte((byte) 0b0000_0000);
            }
            count -= 8;
        }

        // buffer remaining bits
        if (count > 0) {
            if (value) {
                data = getLowBitMask(count) << (8 - count);
            }
            bitsInData = count;
        }
    }

    private void flushData()
    {
        byteOutputStream.writeByte((byte) data);
        data = 0;
        bitsInData = 0;
    }

    @Override
    public void recordCheckpoint()
    {
        checkState(!closed);
        byteOutputStream.recordCheckpoint();
        checkpointBitOffsets.add(bitsInData);
    }

    @Override
    public void close()
    {
        closed = true;
        if (bitsInData > 0) {
            flushData();
        }
        byteOutputStream.close();
    }

    @Override
    public List<BooleanStreamCheckpoint> getCheckpoints()
    {
        checkState(closed);
        ImmutableList.Builder<BooleanStreamCheckpoint> booleanStreamCheckpoint = ImmutableList.builder();
        List<ByteStreamCheckpoint> byteStreamCheckpoints = byteOutputStream.getCheckpoints();
        for (int groupId = 0; groupId < checkpointBitOffsets.size(); groupId++) {
            int checkpointBitOffset = checkpointBitOffsets.get(groupId);
            ByteStreamCheckpoint byteStreamCheckpoint = byteStreamCheckpoints.get(groupId);
            booleanStreamCheckpoint.add(new BooleanStreamCheckpoint(checkpointBitOffset, byteStreamCheckpoint));
        }
        return booleanStreamCheckpoint.build();
    }

    @Override
    public Optional<Stream> writeDataStreams(int column, SliceOutput outputStream)
    {
        checkState(closed);
        return byteOutputStream.writeDataStreams(column, outputStream);
    }

    @Override
    public long getBufferedBytes()
    {
        return byteOutputStream.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        // NOTE: we do not include checkpoints because they should be small and it would be annoying to calculate the size
        return INSTANCE_SIZE + byteOutputStream.getRetainedBytes();
    }

    @Override
    public void reset()
    {
        data = 0;
        bitsInData = 0;

        closed = false;
        byteOutputStream.reset();
        checkpointBitOffsets.clear();
    }

    private static int getLowBitMask(int bits)
    {
        return (0x1 << bits) - 1;
    }
}

