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

import static com.splicemachine.orc.metadata.statistics.DoubleStatistics.DOUBLE_VALUE_BYTES;
import static java.util.Objects.requireNonNull;

public class DoubleStatisticsBuilder
        implements StatisticsBuilder
{
    private long nonNullValueCount;
    private boolean hasNan;
    private double minimum = Double.POSITIVE_INFINITY;
    private double maximum = Double.NEGATIVE_INFINITY;

    public static DoubleStatisticsBuilder newBuilder() {
        return new DoubleStatisticsBuilder();
    }

    public void addValue(double value)
    {
        nonNullValueCount++;
        if (Double.isNaN(value)) {
            hasNan = true;
        }
        else {
            minimum = Math.min(value, minimum);
            maximum = Math.max(value, maximum);
        }
    }

    private void addDoubleStatistics(long valueCount, DoubleStatistics value)
    {
        requireNonNull(value, "value is null");
        requireNonNull(value.getMin(), "value.getMin() is null");
        requireNonNull(value.getMax(), "value.getMax() is null");

        nonNullValueCount += valueCount;
        minimum = Math.min(value.getMin(), minimum);
        maximum = Math.max(value.getMax(), maximum);
    }

    private Optional<DoubleStatistics> buildDoubleStatistics()
    {
        // if there are NaN values we can not say anything about the data
        if (nonNullValueCount == 0 || hasNan) {
            return Optional.empty();
        }
        return Optional.of(new DoubleStatistics(minimum, maximum));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<DoubleStatistics> doubleStatistics = buildDoubleStatistics();
        return new ColumnStatistics(
                nonNullValueCount,
                doubleStatistics.map(s -> DOUBLE_VALUE_BYTES).orElse(0L),
                null,
                null,
                doubleStatistics.orElse(null),
                null,
                null,
                null,
                null,
                null);
    }

    @Override
    public ColumnStatistics buildColumnPartitionStatistics(String value) {
        addValue(Double.valueOf(value));
        return buildColumnStatistics();
    }

    public static Optional<DoubleStatistics> mergeDoubleStatistics(List<ColumnStatistics> stats)
    {
        DoubleStatisticsBuilder doubleStatisticsBuilder = new DoubleStatisticsBuilder();
        for (ColumnStatistics columnStatistics : stats) {
            DoubleStatistics partialStatistics = columnStatistics.getDoubleStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                doubleStatisticsBuilder.addDoubleStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return doubleStatisticsBuilder.buildDoubleStatistics();
    }
}

