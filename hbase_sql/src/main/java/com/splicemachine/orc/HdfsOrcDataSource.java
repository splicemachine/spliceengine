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
package com.splicemachine.orc;

import io.airlift.units.DataSize;
import org.apache.hadoop.fs.FSDataInputStream;
import java.io.IOException;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HdfsOrcDataSource
        extends AbstractOrcDataSource
{
    private final FSDataInputStream inputStream;
    private final FileFormatDataSourceStats stats;

    public HdfsOrcDataSource(
            OrcDataSourceId id,
            long size,
            DataSize maxMergeDistance,
            DataSize maxReadSize,
            DataSize streamBufferSize,
            FSDataInputStream inputStream,
            FileFormatDataSourceStats stats)
    {
        super(id, size, maxMergeDistance, maxReadSize, streamBufferSize);
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public void close()
            throws IOException
    {
        inputStream.close();
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        try {
            long readStart = System.nanoTime();
            inputStream.readFully(position, buffer, bufferOffset, bufferLength);
            stats.readDataBytesPerSecond(bufferLength, System.nanoTime() - readStart);
        }
        catch (Exception e) {
            String message = format("Error reading from %s at position %s", this, position);
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new IOException(message, e);
            }
            throw new IOException(message, e);
        }
    }
}

