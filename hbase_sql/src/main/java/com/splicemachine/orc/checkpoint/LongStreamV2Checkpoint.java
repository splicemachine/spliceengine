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
package com.splicemachine.orc.checkpoint;

import com.splicemachine.orc.checkpoint.Checkpoints.ColumnPositionsList;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.splicemachine.orc.checkpoint.InputStreamCheckpoint.*;
import static com.google.common.base.MoreObjects.toStringHelper;

public final class LongStreamV2Checkpoint
        implements LongStreamCheckpoint
{
    private final int offset;
    private final long inputStreamCheckpoint;

    public LongStreamV2Checkpoint(int offset, long inputStreamCheckpoint)
    {
        this.offset = offset;
        this.inputStreamCheckpoint = inputStreamCheckpoint;
    }

    public LongStreamV2Checkpoint(boolean compressed, ColumnPositionsList positionsList)
    {
        inputStreamCheckpoint = createInputStreamCheckpoint(compressed, positionsList);
        offset = positionsList.nextPosition();
    }

    public int getOffset()
    {
        return offset;
    }

    public long getInputStreamCheckpoint()
    {
        return inputStreamCheckpoint;
    }

    @Override
    public List<Integer> toPositionList(boolean compressed)
    {
        return ImmutableList.<Integer>builder()
                .addAll(createInputStreamPositionList(compressed, inputStreamCheckpoint))
                .add(offset)
                .build();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("offset", offset)
                .add("inputStreamCheckpoint", inputStreamCheckpointToString(inputStreamCheckpoint))
                .toString();
    }
}

