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
import java.util.List;
import java.util.Optional;

import static com.splicemachine.orc.metadata.statistics.DecimalStatistics.DECIMAL_VALUE_BYTES_OVERHEAD;
import static java.util.Objects.requireNonNull;

public class LongDecimalStatisticsBuilder
        implements StatisticsBuilder
{
    public static final long LONG_DECIMAL_VALUE_BYTES = 16L;

    private long nonNullValueCount;
    private BigDecimal minimum;
    private BigDecimal maximum;

    public static LongDecimalStatisticsBuilder newBuilder() {
        return new LongDecimalStatisticsBuilder();
    }

    public void addValue(BigDecimal value)
    {
        requireNonNull(value, "value is null");

        nonNullValueCount++;

        if (minimum == null) {
            minimum = value;
            maximum = value;
        }
        else {
            minimum = minimum.min(value);
            maximum = maximum.max(value);
        }
    }

    private void addDecimalStatistics(long valueCount, DecimalStatistics value)
    {
        requireNonNull(value, "value is null");
        requireNonNull(value.getMin(), "value.getMin() is null");
        requireNonNull(value.getMax(), "value.getMax() is null");

        nonNullValueCount += valueCount;
        if (minimum == null) {
            minimum = value.getMin();
            maximum = value.getMax();
        }
        else {
            minimum = minimum.min(value.getMin());
            maximum = maximum.max(value.getMax());
        }
    }

    private Optional<DecimalStatistics> buildDecimalStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new DecimalStatistics(minimum, maximum));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<DecimalStatistics> decimalStatistics = buildDecimalStatistics();
        return new ColumnStatistics(
                nonNullValueCount,
                decimalStatistics.map(s -> DECIMAL_VALUE_BYTES_OVERHEAD + LONG_DECIMAL_VALUE_BYTES).orElse(0L),
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
        addValue(BigDecimal.valueOf(Double.parseDouble(value)));
        return buildColumnStatistics();
    }

    public static Optional<DecimalStatistics> mergeDecimalStatistics(List<ColumnStatistics> stats)
    {
        LongDecimalStatisticsBuilder decimalStatisticsBuilder = new LongDecimalStatisticsBuilder();
        for (ColumnStatistics columnStatistics : stats) {
            DecimalStatistics partialStatistics = columnStatistics.getDecimalStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                decimalStatisticsBuilder.addDecimalStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return decimalStatisticsBuilder.buildDecimalStatistics();
    }
}

