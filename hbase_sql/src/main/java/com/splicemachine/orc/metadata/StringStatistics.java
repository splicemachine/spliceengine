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
package com.splicemachine.orc.metadata;

import com.splicemachine.orc.input.SpliceOrcNewInputFormat;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;

public class StringStatistics
        implements RangeStatistics<Slice>
{
    private final Slice minimum;
    private final Slice maximum;

    public StringStatistics(Slice minimum, Slice maximum)
    {
        this.minimum = minimum;
        this.maximum = maximum;
    }

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

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("min", minimum)
                .add("max", maximum)
                .toString();
    }

    public static ColumnStatistics getPartitionColumnStatistics(String value) throws IOException {
        StringStatistics stringStatistics = null;
        if(value != null) {
            Slice slice = Slices.wrappedBuffer(value.getBytes("UTF-8"));
            stringStatistics = new StringStatistics(slice,slice);
        }
        return new ColumnStatistics(SpliceOrcNewInputFormat.DEFAULT_PARTITION_SIZE,
                null,null,null,stringStatistics,null,null,null);
    }

}
