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
import com.splicemachine.orc.checkpoint.BooleanStreamCheckpoint;
import com.splicemachine.orc.metadata.CompressionKind;
import com.splicemachine.orc.metadata.Stream;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.splicemachine.orc.metadata.Stream.StreamKind.PRESENT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class PresentOutputStream
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PresentOutputStream.class).instanceSize();
    private final OrcOutputBuffer buffer;

    // boolean stream will only exist if null values being recorded
    @Nullable
    private BooleanOutputStream booleanOutputStream;

    private final List<Integer> groupsCounts = new ArrayList<>();
    private int currentGroupCount;

    private boolean closed;

    public PresentOutputStream(CompressionKind compression, int bufferSize)
    {
        this.buffer = new OrcOutputBuffer(compression, bufferSize);
    }

    public void writeBoolean(boolean value)
    {
        checkArgument(!closed);
        if (!value && booleanOutputStream == null) {
            createBooleanOutputStream();
        }

        if (booleanOutputStream != null) {
            booleanOutputStream.writeBoolean(value);
        }
        currentGroupCount++;
    }

    private void createBooleanOutputStream()
    {
        checkState(booleanOutputStream == null);
        booleanOutputStream = new BooleanOutputStream(buffer);
        for (int groupsCount : groupsCounts) {
            booleanOutputStream.writeBooleans(groupsCount, true);
            booleanOutputStream.recordCheckpoint();
        }
        booleanOutputStream.writeBooleans(currentGroupCount, true);
    }

    public void recordCheckpoint()
    {
        checkArgument(!closed);
        groupsCounts.add(currentGroupCount);
        currentGroupCount = 0;

        if (booleanOutputStream != null) {
            booleanOutputStream.recordCheckpoint();
        }
    }

    public void close()
    {
        closed = true;
        if (booleanOutputStream != null) {
            booleanOutputStream.close();
        }
    }

    public Optional<List<BooleanStreamCheckpoint>> getCheckpoints()
    {
        checkArgument(closed);
        if (booleanOutputStream == null) {
            return Optional.empty();
        }
        return Optional.of(booleanOutputStream.getCheckpoints());
    }

    public Optional<Stream> writeDataStreams(int column, SliceOutput outputStream)
    {
        checkArgument(closed);
        if (booleanOutputStream == null) {
            return Optional.empty();
        }
        Stream stream = booleanOutputStream.writeDataStreams(column, outputStream).get();
        return Optional.of(new Stream(stream.getColumn(), PRESENT, stream.getLength(), stream.isUseVInts()));
    }

    public long getBufferedBytes()
    {
        if (booleanOutputStream == null) {
            return 0;
        }
        return booleanOutputStream.getBufferedBytes();
    }

    public long getRetainedBytes()
    {
        // NOTE: we do not include checkpoints because they should be small and it would be annoying to calculate the size
        if (booleanOutputStream == null) {
            return INSTANCE_SIZE + buffer.getRetainedSize();
        }
        return INSTANCE_SIZE + booleanOutputStream.getRetainedBytes();
    }

    public void reset()
    {
        closed = false;
        booleanOutputStream = null;
        buffer.reset();
        groupsCounts.clear();
        currentGroupCount = 0;
    }
}

