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
package com.splicemachine.orc.metadata.statistics;

import com.splicemachine.orc.input.SpliceOrcNewInputFormat;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class StringStatistics
        implements RangeStatistics<Slice>
{
    // 1 byte to denote if null + 4 bytes to denote offset
    public static final long STRING_VALUE_BYTES_OVERHEAD = Byte.BYTES + Integer.BYTES;

    @Nullable
    private final Slice minimum;
    @Nullable
    private final Slice maximum;
    private final long sum;

    public StringStatistics(Slice minimum, Slice maximum, long sum)
    {
        checkArgument(minimum == null || maximum == null || minimum.compareTo(maximum) <= 0, "minimum is not less than maximum");
        this.minimum = minimum;
        this.maximum = maximum;
        this.sum = sum;
    }
/* msirek-temp->
    public static ColumnStatistics getPartitionColumnStatistics(String value) throws IOException {
        StringStatistics stringStatistics = null;
        if(value != null) {
            Slice slice = Slices.wrappedBuffer(value.getBytes("UTF-8"));
            stringStatistics = new StringStatistics(slice,slice);
        }
        return new ColumnStatistics(SpliceOrcNewInputFormat.DEFAULT_PARTITION_SIZE,
            null,null,null,stringStatistics,null,null,null);
    }
    */

    @Override
    public Slice getMin()
    {
        return minimum;
    }

    @Override
    public Slice getMax()
    {
        return maximum;
    }

    public long getSum()
    {
        return sum;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StringStatistics that = (StringStatistics) o;
        return Objects.equals(minimum, that.minimum) &&
                Objects.equals(maximum, that.maximum);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(minimum, maximum);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("min", minimum == null ? "<null>" : minimum.toStringUtf8())
                .add("max", maximum == null ? "<null>" : maximum.toStringUtf8())
                .add("sum", sum)
                .toString();
    }
}

