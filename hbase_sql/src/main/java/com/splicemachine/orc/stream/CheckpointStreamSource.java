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

import com.splicemachine.orc.checkpoint.StreamCheckpoint;

import javax.annotation.Nullable;
import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CheckpointStreamSource<S extends ValueStream<C>, C extends StreamCheckpoint>
        implements StreamSource<S>
{
    public static <S extends ValueStream<C>, C extends StreamCheckpoint> CheckpointStreamSource<S, C> createCheckpointStreamSource(S stream, StreamCheckpoint checkpoint)
    {
        requireNonNull(stream, "stream is null");
        requireNonNull(checkpoint, "checkpoint is null");

        Class<? extends C> checkpointType = stream.getCheckpointType();
        C verifiedCheckpoint = (C) checkpoint;
        return new CheckpointStreamSource<>(stream, verifiedCheckpoint);
    }

    private final S stream;
    private final C checkpoint;

    public CheckpointStreamSource(S stream, C checkpoint)
    {
        this.stream = requireNonNull(stream, "stream is null");
        this.checkpoint = requireNonNull(checkpoint, "checkpoint is null");
    }

    @Override
    public Class<S> getStreamType()
    {
        return (Class<S>) stream.getClass();
    }

    @Nullable
    @Override
    public S openStream()
            throws IOException
    {
        stream.seekToCheckpoint(checkpoint);
        return stream;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stream", stream)
                .add("checkpoint", checkpoint)
                .toString();
    }
}
