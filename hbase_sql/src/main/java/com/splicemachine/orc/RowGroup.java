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

import com.splicemachine.orc.stream.StreamSources;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class RowGroup
{
    private final int groupId;
    private final long rowOffset;
    private final long rowCount;
    private final StreamSources streamSources;

    public RowGroup(int groupId, long rowOffset, long rowCount, StreamSources streamSources)
    {
        this.groupId = groupId;
        this.rowOffset = rowOffset;
        this.rowCount = rowCount;
        this.streamSources = requireNonNull(streamSources, "streamSources is null");
    }

    public int getGroupId()
    {
        return groupId;
    }

    public long getRowOffset()
    {
        return rowOffset;
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public StreamSources getStreamSources()
    {
        return streamSources;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("groupId", groupId)
                .add("rowOffset", rowOffset)
                .add("rowCount", rowCount)
                .add("streamSources", streamSources)
                .toString();
    }
}
