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
package com.splicemachine.orc.stream;

import com.splicemachine.orc.StreamDescriptor;
import com.splicemachine.orc.StreamId;
import com.splicemachine.orc.metadata.Stream.StreamKind;
import com.google.common.collect.ImmutableMap;
import javax.annotation.Nonnull;
import java.util.Map;
import static com.google.common.base.Preconditions.checkArgument;
import static com.splicemachine.orc.stream.MissingStreamSource.missingStreamSource;
import static java.util.Objects.requireNonNull;

public class StreamSources
{
    private final Map<StreamId, StreamSource<?>> streamSources;

    public StreamSources(Map<StreamId, StreamSource<?>> streamSources)
    {
        this.streamSources = ImmutableMap.copyOf(requireNonNull(streamSources, "streamSources is null"));
    }

    @Nonnull
    public <S extends ValueStream<?>> StreamSource<S> getStreamSource(StreamDescriptor streamDescriptor, StreamKind streamKind, Class<S> streamType)
    {
        requireNonNull(streamDescriptor, "streamDescriptor is null");
        requireNonNull(streamType, "streamType is null");

        StreamSource<?> streamSource = streamSources.get(new StreamId(streamDescriptor.getStreamId(), streamKind));
        if (streamSource == null) {
            streamSource = missingStreamSource(streamType);
        }

        checkArgument(streamType.isAssignableFrom(streamSource.getStreamType()),
                "%s must be of type %s, not %s",
                streamDescriptor,
                streamType.getName(),
                streamSource.getStreamType().getName());

        return (StreamSource<S>) streamSource;
    }
}
