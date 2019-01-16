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

import org.apache.hadoop.hive.serde2.io.DateWritable;

import java.util.List;
import java.util.Optional;

import static com.splicemachine.orc.metadata.statistics.DateStatistics.DATE_VALUE_BYTES;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DateStatisticsBuilder
        implements LongValueStatisticsBuilder
{
    private long nonNullValueCount;
    private int minimum = Integer.MAX_VALUE;
    private int maximum = Integer.MIN_VALUE;

    public static DateStatisticsBuilder newBuilder() {
        return new DateStatisticsBuilder();
    }

    @Override
    public void addValue(long value)
    {
        nonNullValueCount++;

        int intValue = toIntExact(value);
        minimum = Math.min(intValue, minimum);
        maximum = Math.max(intValue, maximum);
    }

    private void addDateStatistics(long valueCount, DateStatistics value)
    {
        requireNonNull(value, "value is null");
        requireNonNull(value.getMin(), "value.getMin() is null");
        requireNonNull(value.getMax(), "value.getMax() is null");

        nonNullValueCount += valueCount;
        minimum = Math.min(value.getMin(), minimum);
        maximum = Math.max(value.getMax(), maximum);
    }

    private Optional<DateStatistics> buildDateStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new DateStatistics(minimum, maximum));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<DateStatistics> dateStatistics = buildDateStatistics();
        return new ColumnStatistics(
                nonNullValueCount,
                dateStatistics.map(s -> DATE_VALUE_BYTES).orElse(0L),
                null,
                null,
                null,
                null,
                dateStatistics.orElse(null),
                null,
                null,
                null);
    }

    @Override
    public ColumnStatistics buildColumnPartitionStatistics(String value) {
        Integer dateInDays = DateWritable.dateToDays(java.sql.Date.valueOf(value));
        addValue(dateInDays);
        return buildColumnStatistics();
    }

    public static Optional<DateStatistics> mergeDateStatistics(List<ColumnStatistics> stats)
    {
        DateStatisticsBuilder dateStatisticsBuilder = new DateStatisticsBuilder();
        for (ColumnStatistics columnStatistics : stats) {
            DateStatistics partialStatistics = columnStatistics.getDateStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                dateStatisticsBuilder.addDateStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return dateStatisticsBuilder.buildDateStatistics();
    }
}

