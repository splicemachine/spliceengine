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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Optional;

import static com.splicemachine.orc.metadata.statistics.DecimalStatistics.DECIMAL_VALUE_BYTES_OVERHEAD;

public class ShortDecimalStatisticsBuilder
        implements LongValueStatisticsBuilder
{
    public static final long SHORT_DECIMAL_VALUE_BYTES = 8L;

    private final int scale;

    private long nonNullValueCount;
    private long minimum = Long.MAX_VALUE;
    private long maximum = Long.MIN_VALUE;

    public static ShortDecimalStatisticsBuilder newBuilder(int scale) {
        return new ShortDecimalStatisticsBuilder(scale);
    }

    public ShortDecimalStatisticsBuilder(int scale)
    {
        this.scale = scale;
    }

    @Override
    public void addValue(long value)
    {
        nonNullValueCount++;

        minimum = Math.min(value, minimum);
        maximum = Math.max(value, maximum);
    }

    private Optional<DecimalStatistics> buildDecimalStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new DecimalStatistics(
                new BigDecimal(BigInteger.valueOf(minimum), scale),
                new BigDecimal(BigInteger.valueOf(maximum), scale)));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<DecimalStatistics> decimalStatistics = buildDecimalStatistics();
        return new ColumnStatistics(
                nonNullValueCount,
                decimalStatistics.map(s -> DECIMAL_VALUE_BYTES_OVERHEAD + SHORT_DECIMAL_VALUE_BYTES).orElse(0L),
                null,
                null,
                null,
                null,
                null,
                decimalStatistics.orElse(null),
                null,
                null);
    }

    @Override
    public ColumnStatistics buildColumnPartitionStatistics(String value) {
        addValue(Long.valueOf(value));
        return buildColumnStatistics();
    }
}

