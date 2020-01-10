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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.FixedLengthSliceInput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import static java.util.Objects.requireNonNull;

class TestingOrcDataSource
        implements OrcDataSource
{
    private final OrcDataSource delegate;

    private int readCount;
    private List<DiskRange> lastReadRanges;

    public TestingOrcDataSource(OrcDataSource delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    public int getReadCount()
    {
        return readCount;
    }

    public List<DiskRange> getLastReadRanges()
    {
        return lastReadRanges;
    }

    @Override
    public long getReadBytes()
    {
        return delegate.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public long getSize()
    {
        return delegate.getSize();
    }

    @Override
    public void readFully(long position, byte[] buffer)
            throws IOException
    {
        readCount++;
        lastReadRanges = ImmutableList.of(new DiskRange(position, buffer.length));
        delegate.readFully(position, buffer);
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        readCount++;
        lastReadRanges = ImmutableList.of(new DiskRange(position, bufferLength));
        delegate.readFully(position, buffer, bufferOffset, bufferLength);
    }

    @Override
    public <K> Map<K, FixedLengthSliceInput> readFully(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        readCount += diskRanges.size();
        lastReadRanges = ImmutableList.copyOf(diskRanges.values());
        return delegate.readFully(diskRanges);
    }
}
