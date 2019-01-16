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
import org.apache.hadoop.hive.serde2.io.DateWritable;

import java.io.IOException;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class DateStatistics
        implements RangeStatistics<Integer>
{
    // 1 byte to denote if null + 4 bytes for the value (date is of integer type)
    public static final long DATE_VALUE_BYTES = Byte.BYTES + Integer.BYTES;

    private final Integer minimum;
    private final Integer maximum;

    public DateStatistics(Integer minimum, Integer maximum)
    {
        checkArgument(minimum == null || maximum == null || minimum <= maximum, "minimum is not less than maximum");
        this.minimum = minimum;
        this.maximum = maximum;
    }

    /* msirek-temp->
    public static ColumnStatistics getPartitionColumnStatistics(String value) throws IOException {
        DateStatistics dateStatistics = null;
        if(value != null) {
            Integer dateInDays = DateWritable.dateToDays(java.sql.Date.valueOf(value));
            dateStatistics = new DateStatistics(dateInDays, dateInDays);
        }
        return new ColumnStatistics(SpliceOrcNewInputFormat.DEFAULT_PARTITION_SIZE,
            null,null,null,null,dateStatistics,null,null);
    }
    */

    @Override
    public Integer getMin()
    {
        return minimum;
    }

    @Override
    public Integer getMax()
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
        DateStatistics that = (DateStatistics) o;
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

