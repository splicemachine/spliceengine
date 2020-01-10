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

import com.splicemachine.orc.checkpoint.ByteArrayStreamCheckpoint;

import java.io.IOException;

import static com.splicemachine.orc.stream.OrcStreamUtils.readFully;
import static com.splicemachine.orc.stream.OrcStreamUtils.skipFully;
import static java.util.Objects.requireNonNull;

public class ByteArrayStream
        implements ValueStream<ByteArrayStreamCheckpoint>
{
    private final OrcInputStream inputStream;

    public ByteArrayStream(OrcInputStream inputStream)
    {
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
    }

    public byte[] next(int length)
            throws IOException
    {
        byte[] data = new byte[length];
        readFully(inputStream, data, 0, length);
        return data;
    }

    public void next(int length, byte[] data)
            throws IOException
    {
        readFully(inputStream, data, 0, length);
    }

    @Override
    public Class<ByteArrayStreamCheckpoint> getCheckpointType()
    {
        return ByteArrayStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(ByteArrayStreamCheckpoint checkpoint)
            throws IOException
    {
        inputStream.seekToCheckpoint(checkpoint.getInputStreamCheckpoint());
    }

    @Override
    public void skip(long skipSize)
            throws IOException
    {
        skipFully(inputStream, skipSize);
    }
}
