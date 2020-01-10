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
package com.splicemachine.orc.checkpoint;

import com.splicemachine.orc.checkpoint.Checkpoints.ColumnPositionsList;
import com.splicemachine.orc.metadata.CompressionKind;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class BooleanStreamCheckpoint
        implements StreamCheckpoint
{
    private final int offset;
    private final ByteStreamCheckpoint byteStreamCheckpoint;

    public BooleanStreamCheckpoint(int offset, ByteStreamCheckpoint byteStreamCheckpoint)
    {
        this.offset = offset;
        this.byteStreamCheckpoint = requireNonNull(byteStreamCheckpoint, "byteStreamCheckpoint is null");
    }

    public BooleanStreamCheckpoint(CompressionKind compressionKind, ColumnPositionsList positionsList)
    {
        byteStreamCheckpoint = new ByteStreamCheckpoint(compressionKind, positionsList);
        offset = positionsList.nextPosition();
    }

    public int getOffset()
    {
        return offset;
    }

    public ByteStreamCheckpoint getByteStreamCheckpoint()
    {
        return byteStreamCheckpoint;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("offset", offset)
                .add("byteStreamCheckpoint", byteStreamCheckpoint)
                .toString();
    }
}
