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
import com.splicemachine.orc.checkpoint.ByteArrayStreamCheckpoint;
import com.splicemachine.orc.metadata.CompressionKind;
import com.splicemachine.orc.metadata.Stream;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import static com.splicemachine.orc.metadata.Stream.StreamKind.DATA;
import static com.google.common.base.Preconditions.checkState;

/**
 * This is a concatenation of all byte array content, and a separate length stream will be used to get the boundaries.
 */
public class ByteArrayOutputStream
        implements ValueOutputStream<ByteArrayStreamCheckpoint>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ByteArrayOutputStream.class).instanceSize();
    private final OrcOutputBuffer buffer;
    private final List<ByteArrayStreamCheckpoint> checkpoints = new ArrayList<>();
    private final Stream.StreamKind streamKind;

    private boolean closed;

    public ByteArrayOutputStream(CompressionKind compression, int bufferSize)
    {
        this(compression, bufferSize, DATA);
    }

    public ByteArrayOutputStream(CompressionKind compression, int bufferSize, Stream.StreamKind streamKind)
    {
        this.buffer = new OrcOutputBuffer(compression, bufferSize);
        this.streamKind = streamKind;
    }

    public void writeSlice(Slice value)
    {
        checkState(!closed);
        buffer.writeBytes(value);
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public void recordCheckpoint()
    {
        checkState(!closed);
        checkpoints.add(new ByteArrayStreamCheckpoint(buffer.getCheckpoint()));
    }

    @Override
    public List<ByteArrayStreamCheckpoint> getCheckpoints()
    {
        checkState(closed);
        return ImmutableList.copyOf(checkpoints);
    }

    @Override
    public Optional<Stream> writeDataStreams(int column, SliceOutput outputStream)
    {
        checkState(closed);
        int length = buffer.writeDataTo(outputStream);
        return Optional.of(new Stream(column, streamKind, length, false));
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

