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

import com.splicemachine.db.shared.common.sanity.SanityManager;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Optional;

import static com.splicemachine.orc.metadata.statistics.StringStatistics.STRING_VALUE_BYTES_OVERHEAD;
import static java.util.Objects.requireNonNull;

public class StringStatisticsBuilder
        implements SliceColumnStatisticsBuilder
{
    private long nonNullValueCount;
    private Slice minimum;
    private Slice maximum;
    private long sum;

    public long getNonNullValueCount()
    {
        return nonNullValueCount;
    }

    public static StringStatisticsBuilder newBuilder() {
        return new StringStatisticsBuilder();
    }

    @Override
    public void addValue(Slice value)
    {
        requireNonNull(value, "value is null");

        nonNullValueCount++;
        sum += value.length();
        updateMinMax(value);
    }

    private void updateMinMax(Slice value)
    {
        if (minimum == null) {
            minimum = value;
            maximum = value;
        }
        else if (value.compareTo(minimum) <= 0) {
            minimum = value;
        }
        else if (value.compareTo(maximum) >= 0) {
            maximum = value;
        }
    }

    private void addStringStatistics(long valueCount, StringStatistics value)
    {
        requireNonNull(value, "value is null");
        requireNonNull(value.getMin(), "value.getMin() is null");
        requireNonNull(value.getMax(), "value.getMax() is null");

        nonNullValueCount += valueCount;
        sum += value.getSum();
        updateMinMax(value.getMin());
        updateMinMax(value.getMax());
    }

    private Optional<StringStatistics> buildStringStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new StringStatistics(minimum, maximum, sum));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<StringStatistics> stringStatistics = buildStringStatistics();
        stringStatistics.ifPresent(s -> SanityManager.ASSERT(nonNullValueCount > 0));
        return new ColumnStatistics(
                nonNullValueCount,
                stringStatistics.map(s -> STRING_VALUE_BYTES_OVERHEAD + sum / nonNullValueCount).orElse(0L),
                null,
                null,
                null,
                stringStatistics.orElse(null),
                null,
                null,
                null,
                null);
    }

    @Override
    public ColumnStatistics buildColumnPartitionStatistics(String value) {
        try {
            addValue(Slices.wrappedBuffer(value.getBytes("UTF-8")));
            return buildColumnStatistics();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Optional<StringStatistics> mergeStringStatistics(List<ColumnStatistics> stats)
    {
        StringStatisticsBuilder stringStatisticsBuilder = new StringStatisticsBuilder();
        for (ColumnStatistics columnStatistics : stats) {
            StringStatistics partialStatistics = columnStatistics.getStringStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                stringStatisticsBuilder.addStringStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return stringStatisticsBuilder.buildStringStatistics();
    }
}

