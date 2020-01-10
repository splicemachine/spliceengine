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

import io.airlift.units.DataSize;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileOrcDataSource
        extends AbstractOrcDataSource
{
    private final RandomAccessFile input;

    public FileOrcDataSource(File path, DataSize maxMergeDistance, DataSize maxReadSize, DataSize streamBufferSize)
            throws FileNotFoundException
    {
        super(path.getPath(), path.length(), maxMergeDistance, maxReadSize, streamBufferSize);
        this.input = new RandomAccessFile(path, "r");
    }

    @Override
    public void close()
            throws IOException
    {
        input.close();
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        input.seek(position);
        input.readFully(buffer, bufferOffset, bufferLength);
    }
}
