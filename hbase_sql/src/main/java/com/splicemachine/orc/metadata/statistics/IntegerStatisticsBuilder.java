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

import java.util.List;
import java.util.Optional;

import static com.splicemachine.orc.metadata.statistics.IntegerStatistics.INTEGER_VALUE_BYTES;
import static java.util.Objects.requireNonNull;

public class IntegerStatisticsBuilder
        implements LongValueStatisticsBuilder
{
    private long nonNullValueCount;
    private long minimum = Long.MAX_VALUE;
    private long maximum = Long.MIN_VALUE;


    public static IntegerStatisticsBuilder newBuilder() {
        return new IntegerStatisticsBuilder();
    }

    @Override
    public void addValue(long value)
    {
        nonNullValueCount++;

        minimum = Math.min(value, minimum);
        maximum = Math.max(value, maximum);
    }

    private void addIntegerStatistics(long valueCount, IntegerStatistics value)
    {
        requireNonNull(value, "value is null");
        requireNonNull(value.getMin(), "value.getMin() is null");
        requireNonNull(value.getMax(), "value.getMax() is null");

        nonNullValueCount += valueCount;
        minimum = Math.min(value.getMin(), minimum);
        maximum = Math.max(value.getMax(), maximum);
    }

    private Optional<IntegerStatistics> buildIntegerStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new IntegerStatistics(minimum, maximum));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<IntegerStatistics> integerStatistics = buildIntegerStatistics();
        return new ColumnStatistics(
                nonNullValueCount,
                integerStatistics.map(s -> INTEGER_VALUE_BYTES).orElse(0L),
                null,
                integerStatistics.orElse(null),
                null,
                null,
                null,
                null,
                null,
                null);
    }

    @Override
    public ColumnStatistics buildColumnPartitionStatistics(String value) {
        addValue(Long.valueOf(value));
        return buildColumnStatistics();
    }

    public static Optional<IntegerStatistics> mergeIntegerStatistics(List<ColumnStatistics> stats)
    {
        IntegerStatisticsBuilder integerStatisticsBuilder = new IntegerStatisticsBuilder();
        for (ColumnStatistics columnStatistics : stats) {
            IntegerStatistics partialStatistics = columnStatistics.getIntegerStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                integerStatisticsBuilder.addIntegerStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return integerStatisticsBuilder.buildIntegerStatistics();
    }
}

