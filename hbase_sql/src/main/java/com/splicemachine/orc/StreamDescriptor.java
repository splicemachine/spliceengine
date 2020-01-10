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

import com.splicemachine.orc.metadata.OrcType.OrcTypeKind;
import com.google.common.collect.ImmutableList;
import java.util.List;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class StreamDescriptor
{
    private final String streamName;
    private final int streamId;
    private final OrcTypeKind streamType;
    private final String fieldName;
    private final OrcDataSource fileInput;
    private final List<StreamDescriptor> nestedStreams;

    public StreamDescriptor(String streamName, int streamId, String fieldName, OrcTypeKind streamType, OrcDataSource fileInput, List<StreamDescriptor> nestedStreams)
    {
        this.streamName = requireNonNull(streamName, "streamName is null");
        this.streamId = streamId;
        this.fieldName = requireNonNull(fieldName, "fieldName is null");
        this.streamType = requireNonNull(streamType, "type is null");
        this.fileInput = requireNonNull(fileInput, "fileInput is null");
        this.nestedStreams = ImmutableList.copyOf(requireNonNull(nestedStreams, "nestedStreams is null"));
    }

    public String getStreamName()
    {
        return streamName;
    }

    public int getStreamId()
    {
        return streamId;
    }

    public OrcTypeKind getStreamType()
    {
        return streamType;
    }

    public String getFieldName()
    {
        return fieldName;
    }

    public OrcDataSource getFileInput()
    {
        return fileInput;
    }

    public List<StreamDescriptor> getNestedStreams()
    {
        return nestedStreams;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("streamName", streamName)
                .add("streamId", streamId)
                .add("streamType", streamType)
                .add("path", fileInput)
                .toString();
    }
}
