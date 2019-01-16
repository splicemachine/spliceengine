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

import java.io.IOException;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class DoubleStatistics
        implements RangeStatistics<Double>
{
    // 1 byte to denote if null + 8 bytes for the value
    public static final long DOUBLE_VALUE_BYTES = Byte.BYTES + Double.BYTES;

    private final Double minimum;
    private final Double maximum;

    public DoubleStatistics(Double minimum, Double maximum)
    {
        checkArgument(minimum == null || !minimum.isNaN(), "minimum is NaN");
        checkArgument(maximum == null || !maximum.isNaN(), "maximum is NaN");
        checkArgument(minimum == null || maximum == null || minimum <= maximum, "minimum is not less than maximum");
        this.minimum = minimum;
        this.maximum = maximum;
    }

    /* msirek-temp->
    public static ColumnStatistics getPartitionColumnStatistics(String value) throws IOException {
        DoubleStatistics doubleStatistics = null;
        if(value != null) {
            doubleStatistics = new DoubleStatistics(Double.valueOf(value),Double.valueOf(value));
        }
        return new ColumnStatistics(SpliceOrcNewInputFormat.DEFAULT_PARTITION_SIZE,
            null,null,doubleStatistics,null,null,null,null);
    }
    */

    @Override
    public Double getMin()
    {
        return minimum;
    }

    @Override
    public Double getMax()
    {
        return maximum;
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
        DoubleStatistics that = (DoubleStatistics) o;
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
                .add("min", minimum)
                .add("max", maximum)
                .toString();
    }
}

