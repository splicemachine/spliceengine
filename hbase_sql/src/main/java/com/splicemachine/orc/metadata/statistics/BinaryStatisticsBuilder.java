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

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import java.util.List;
import java.util.Optional;
import static com.splicemachine.orc.metadata.statistics.BinaryStatistics.BINARY_VALUE_BYTES_OVERHEAD;

import static java.util.Objects.requireNonNull;

public class BinaryStatisticsBuilder
        implements SliceColumnStatisticsBuilder
{
    private long nonNullValueCount;
    private long sum;

    public static BinaryStatisticsBuilder newBuilder() {
        return new BinaryStatisticsBuilder();
    }

    @Override
    public void addValue(Slice value)
    {
        requireNonNull(value, "value is null");

        sum += value.length();
        nonNullValueCount++;
    }

    private Optional<BinaryStatistics> buildBinaryStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new BinaryStatistics(sum));
    }

    private void addBinaryStatistics(long valueCount, BinaryStatistics value)
    {
        requireNonNull(value, "value is null");

        nonNullValueCount += valueCount;
        sum += value.getSum();
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<BinaryStatistics> binaryStatistics = buildBinaryStatistics();
        binaryStatistics.ifPresent(s -> SanityManager.ASSERT(nonNullValueCount > 0));
        return new ColumnStatistics(
                nonNullValueCount,
                binaryStatistics.map(s -> BINARY_VALUE_BYTES_OVERHEAD + sum / nonNullValueCount).orElse(0L),
                null,
                null,
                null,
                null,
                null,
                null,
                binaryStatistics.orElse(null),
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

    public static Optional<BinaryStatistics> mergeBinaryStatistics(List<ColumnStatistics> stats)
    {
        BinaryStatisticsBuilder binaryStatisticsBuilder = new BinaryStatisticsBuilder();
        for (ColumnStatistics columnStatistics : stats) {
            BinaryStatistics partialStatistics = columnStatistics.getBinaryStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                binaryStatisticsBuilder.addBinaryStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return binaryStatisticsBuilder.buildBinaryStatistics();
    }
}

