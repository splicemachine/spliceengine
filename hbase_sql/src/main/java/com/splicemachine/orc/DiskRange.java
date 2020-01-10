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

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class DiskRange
{
    private final long offset;
    private final int length;

    public DiskRange(long offset, int length)
    {
        checkArgument(offset >= 0, "offset is negative");
        checkArgument(length >= 0, "length is negative");

        this.offset = offset;
        this.length = length;
    }

    public long getOffset()
    {
        return offset;
    }

    public int getLength()
    {
        return length;
    }

    public long getEnd()
    {
        return offset + length;
    }

    public boolean contains(DiskRange diskRange)
    {
        return offset <= diskRange.getOffset() && diskRange.getEnd() <= getEnd();
    }

    /**
     * Returns the minimal DiskRange that encloses both this DiskRange
     * and otherDiskRange. If there was a gap between the ranges the
     * new range will cover that gap.
     */
    public DiskRange span(DiskRange otherDiskRange)
    {
        requireNonNull(otherDiskRange, "otherDiskRange is null");
        long start = Math.min(this.offset, otherDiskRange.getOffset());
        long end = Math.max(getEnd(), otherDiskRange.getEnd());
        return new DiskRange(start, toIntExact(end - start));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(offset, length);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        DiskRange other = (DiskRange) obj;
        return Objects.equals(this.offset, other.offset)
                && Objects.equals(this.length, other.length);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("offset", offset)
                .add("length", length)
                .toString();
    }
}
