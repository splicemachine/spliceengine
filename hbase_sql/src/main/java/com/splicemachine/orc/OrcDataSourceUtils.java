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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import java.util.*;
import java.util.Map.Entry;

import static java.lang.Math.toIntExact;

public final class OrcDataSourceUtils
{
    private OrcDataSourceUtils()
    {
    }

    /**
     * Merge disk ranges that are closer than {@code maxMergeDistance}.
     */
    public static List<DiskRange> mergeAdjacentDiskRanges(Collection<DiskRange> diskRanges, DataSize maxMergeDistance, DataSize maxReadSize)
    {
        // sort ranges by start offset
        List<DiskRange> ranges = new ArrayList<>(diskRanges);
        Collections.sort(ranges, new Comparator<DiskRange>()
        {
            @Override
            public int compare(DiskRange o1, DiskRange o2)
            {
                return Long.compare(o1.getOffset(), o2.getOffset());
            }
        });

        // merge overlapping ranges
        long maxReadSizeBytes = maxReadSize.toBytes();
        long maxMergeDistanceBytes = maxMergeDistance.toBytes();
        ImmutableList.Builder<DiskRange> result = ImmutableList.builder();
        DiskRange last = ranges.get(0);
        for (int i = 1; i < ranges.size(); i++) {
            DiskRange current = ranges.get(i);
            DiskRange merged = last.span(current);
            if (merged.getLength() <= maxReadSizeBytes && last.getEnd() + maxMergeDistanceBytes >= current.getOffset()) {
                last = merged;
            }
            else {
                result.add(last);
                last = current;
            }
        }
        result.add(last);

        return result.build();
    }

    /**
     * Get a slice for the disk range from the provided buffers.  The buffers ranges do not have
     * to exactly match {@code diskRange}, but {@code diskRange} must be completely contained within
     * one of the buffer ranges.
     */
    public static Slice getDiskRangeSlice(DiskRange diskRange, Map<DiskRange, byte[]> buffers)
    {
        for (Entry<DiskRange, byte[]> bufferEntry : buffers.entrySet()) {
            DiskRange bufferRange = bufferEntry.getKey();
            byte[] buffer = bufferEntry.getValue();
            if (bufferRange.contains(diskRange)) {
                int offset = toIntExact(diskRange.getOffset() - bufferRange.getOffset());
                return Slices.wrappedBuffer(buffer, offset, diskRange.getLength());
            }
        }
        throw new IllegalStateException("No matching buffer for disk range");
    }
}
