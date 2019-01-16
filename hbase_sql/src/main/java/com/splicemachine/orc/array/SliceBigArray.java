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
package com.splicemachine.orc.array;

import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

public final class SliceBigArray
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceBigArray.class).instanceSize();
    private static final int SLICE_INSTANCE_SIZE = ClassLayout.parseClass(Slice.class).instanceSize();
    private final ObjectBigArray<Slice> array;
    private long sizeOfSlices;

    public SliceBigArray()
    {
        array = new ObjectBigArray<>();
    }

    public SliceBigArray(Slice slice)
    {
        array = new ObjectBigArray<>(slice);
    }

    /**
     * Returns the size of this big array in bytes.
     */
    public long sizeOf()
    {
        return INSTANCE_SIZE + array.sizeOf() + sizeOfSlices;
    }

    /**
     * Returns the element of this big array at specified index.
     *
     * @param index a position in this big array.
     * @return the element of this big array at the specified position.
     */
    public Slice get(long index)
    {
        return array.get(index);
    }

    /**
     * Sets the element of this big array at specified index.
     *
     * @param index a position in this big array.
     */
    public void set(long index, Slice value)
    {
        Slice currentValue = array.get(index);
        if (currentValue != null) {
            sizeOfSlices -= getSize(currentValue);
        }
        if (value != null) {
            sizeOfSlices += getSize(value);
        }
        array.set(index, value);
    }

    // For now we approximate the retained size of a slice by object overhead plus length.
    // In general this is more complicated than that as there may be multiple "view" slices
    // pointing to the same backing memory of a slice.
    private long getSize(Slice slice)
    {
        return slice.length() + SLICE_INSTANCE_SIZE;
    }

    /**
     * Ensures this big array is at least the specified length.  If the array is smaller, segments
     * are added until the array is larger then the specified length.
     */
    public void ensureCapacity(long length)
    {
        array.ensureCapacity(length);
    }
}

