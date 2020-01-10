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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.Map;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class CachingOrcDataSource
        implements OrcDataSource
{
    private final OrcDataSource dataSource;
    private final RegionFinder regionFinder;

    private long cachePosition;
    private int cacheLength;
    private byte[] cache;

    public CachingOrcDataSource(OrcDataSource dataSource, RegionFinder regionFinder)
    {
        this.dataSource = requireNonNull(dataSource, "dataSource is null");
        this.regionFinder = requireNonNull(regionFinder, "regionFinder is null");
        this.cache = new byte[0];
    }

    @Override
    public long getReadBytes()
    {
        return dataSource.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return dataSource.getReadTimeNanos();
    }

    @Override
    public long getSize()
    {
        return dataSource.getSize();
    }

    @VisibleForTesting
    void readCacheAt(long offset)
            throws IOException
    {
        DiskRange newCacheRange = regionFinder.getRangeFor(offset);
        cachePosition = newCacheRange.getOffset();
        cacheLength = newCacheRange.getLength();
        if (cache.length < cacheLength) {
            cache = new byte[cacheLength];
        }
        dataSource.readFully(newCacheRange.getOffset(), cache, 0, cacheLength);
    }

    @Override
    public void readFully(long position, byte[] buffer)
            throws IOException
    {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int length)
            throws IOException
    {
        if (position < cachePosition) {
            throw new IllegalArgumentException(String.format("read request (offset %d length %d) is before cache (offset %d length %d)", position, length, cachePosition, cacheLength));
        }
        if (position >= cachePosition + cacheLength) {
            readCacheAt(position);
        }
        if (position + length > cachePosition + cacheLength) {
            throw new IllegalArgumentException(String.format("read request (offset %d length %d) partially overlaps cache (offset %d length %d)", position, length, cachePosition, cacheLength));
        }
        System.arraycopy(cache, toIntExact(position - cachePosition), buffer, bufferOffset, length);
    }

    @Override
    public <K> Map<K, FixedLengthSliceInput> readFully(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        ImmutableMap.Builder<K, FixedLengthSliceInput> builder = ImmutableMap.builder();

        // Assumption here: all disk ranges are in the same region. Therefore, serving them in arbitrary order
        // will not result in eviction of cache that otherwise could have served any of the DiskRanges provided.
        for (Map.Entry<K, DiskRange> entry : diskRanges.entrySet()) {
            DiskRange diskRange = entry.getValue();
            byte[] buffer = new byte[diskRange.getLength()];
            readFully(diskRange.getOffset(), buffer);
            builder.put(entry.getKey(), Slices.wrappedBuffer(buffer).getInput());
        }
        return builder.build();
    }

    @Override
    public void close()
            throws IOException
    {
        dataSource.close();
    }

    @Override
    public String toString()
    {
        return dataSource.toString();
    }

    public interface RegionFinder
    {
        DiskRange getRangeFor(long desiredOffset);
    }
}
