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
import com.splicemachine.orc.OrcOutputBuffer;
import com.splicemachine.orc.checkpoint.ByteStreamCheckpoint;
import com.splicemachine.orc.metadata.CompressionKind;
import com.splicemachine.orc.metadata.Stream;
import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import static com.splicemachine.orc.metadata.Stream.StreamKind.DATA;
import static com.google.common.base.Preconditions.checkState;

public class ByteOutputStream
        implements ValueOutputStream<ByteStreamCheckpoint>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ByteOutputStream.class).instanceSize();

    private static final int MIN_REPEAT_SIZE = 3;
    // A value out side of the range of a signed byte
    private static final int UNMATCHABLE_VALUE = Integer.MAX_VALUE;

    private final OrcOutputBuffer buffer;
    private final List<ByteStreamCheckpoint> checkpoints = new ArrayList<>();

    private final byte[] sequenceBuffer = new byte[128];
    private int size;

    private int runCount;
    private int lastValue = UNMATCHABLE_VALUE;

    private boolean closed;

    public ByteOutputStream(CompressionKind compression, int bufferSize)
    {
        this(new OrcOutputBuffer(compression, bufferSize));
    }

    public ByteOutputStream(OrcOutputBuffer buffer)
    {
        this.buffer = buffer;
    }

    public void writeByte(byte value)
    {
        checkState(!closed);

        // flush if buffer is full
        if (size == sequenceBuffer.length) {
            flushSequence();
        }

        // update run count
        if (value == lastValue) {
            runCount++;
        }
        else {
            // run ended, so flush
            if (runCount >= MIN_REPEAT_SIZE) {
                flushSequence();
            }
            runCount = 1;
        }

        // buffer value
        sequenceBuffer[size] = value;
        size++;

        // before buffering value, check if a run started, and if so flush buffered literal values
        if (runCount == MIN_REPEAT_SIZE && size > MIN_REPEAT_SIZE) {
            // flush the sequence up to the beginning of the run, which must be MIN_REPEAT_SIZE
            size -= MIN_REPEAT_SIZE;
            runCount = 0;

            flushSequence();

            // reset the runCount to the MIN_REPEAT_SIZE
            runCount = MIN_REPEAT_SIZE;
            size = MIN_REPEAT_SIZE;

            // note there is no reason to add the run values to the buffer since is is not used
            // when in a run length sequence
        }

        lastValue = value;
    }

    private void flushSequence()
    {
        if (size == 0) {
            return;
        }

        if (runCount >= MIN_REPEAT_SIZE) {
            buffer.writeByte(runCount - MIN_REPEAT_SIZE);
            buffer.writeByte(lastValue);
        }
        else {
            buffer.writeByte(-size);
            for (int i = 0; i < size; i++) {
                buffer.writeByte(sequenceBuffer[i]);
            }
        }

        size = 0;
        runCount = 0;
        lastValue = UNMATCHABLE_VALUE;
    }

    @Override
    public void recordCheckpoint()
    {
        checkState(!closed);
        checkpoints.add(new ByteStreamCheckpoint(size, buffer.getCheckpoint()));
    }

    @Override
    public void close()
    {
        closed = true;
        flushSequence();
    }

    @Override
    public List<ByteStreamCheckpoint> getCheckpoints()
    {
        checkState(closed);
        return ImmutableList.copyOf(checkpoints);
    }

    @Override
    public Optional<Stream> writeDataStreams(int column, SliceOutput outputStream)
    {
        checkState(closed);
        int length = buffer.writeDataTo(outputStream);
        return Optional.of(new Stream(column, DATA, length, false));
    }

    @Override
    public long getBufferedBytes()
    {
        return buffer.size() + size;
    }

    @Override
    public long getRetainedBytes()
    {
        // NOTE: we do not include checkpoints because they should be small and it would be annoying to calculate the size
        return INSTANCE_SIZE + buffer.getRetainedSize() + SizeOf.sizeOf(sequenceBuffer);
    }

    @Override
    public void reset()
    {
        size = 0;
        runCount = 0;
        lastValue = UNMATCHABLE_VALUE;

        closed = false;
        buffer.reset();
        checkpoints.clear();
    }
}

