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

import java.io.IOException;
import java.math.BigDecimal;

import static com.google.common.base.MoreObjects.toStringHelper;

public class DecimalStatistics
        implements RangeStatistics<BigDecimal>
{
    private final BigDecimal minimum;
    private final BigDecimal maximum;

    public DecimalStatistics(BigDecimal minimum, BigDecimal maximum)
    {
        this.minimum = minimum;
        this.maximum = maximum;
    }

    @Override
    public BigDecimal getMin()
    {
        return minimum;
    }

    @Override
    public BigDecimal getMax()
    {
        return maximum;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("minimum", minimum)
                .add("maximum", maximum)
                .toString();
    }

    public static ColumnStatistics getPartitionColumnStatistics(String value) throws IOException {
        DecimalStatistics decimalStatistics = null;
        if(value != null) {
            BigDecimal bigDecimal = BigDecimal.valueOf(Double.parseDouble(value));
            decimalStatistics = new DecimalStatistics(bigDecimal,
                    bigDecimal);
        }
        return new ColumnStatistics(SpliceOrcNewInputFormat.DEFAULT_PARTITION_SIZE,
                null,null,null,null,null,decimalStatistics,null);
    }


}
