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
package com.splicemachine.orc;

import com.splicemachine.orc.metadata.Stream;
import com.splicemachine.orc.metadata.Stream.StreamKind;

import static com.google.common.base.MoreObjects.toStringHelper;

public final class StreamId
{
    private final int column;
    private final StreamKind streamKind;

    public StreamId(Stream stream)
    {
        this.column = stream.getColumn();
        this.streamKind = stream.getStreamKind();
    }

    public StreamId(int column, StreamKind streamKind)
    {
        this.column = column;
        this.streamKind = streamKind;
    }

    public int getColumn()
    {
        return column;
    }

    public StreamKind getStreamKind()
    {
        return streamKind;
    }

    @Override
    public int hashCode()
    {
        return 31 * column + streamKind.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        StreamId other = (StreamId) obj;
        return column == other.column && streamKind == other.streamKind;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("column", column)
                .add("streamKind", streamKind)
                .toString();
    }
}
