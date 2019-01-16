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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.orc.input.SpliceOrcNewInputFormat;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.splicemachine.orc.metadata.statistics.BooleanStatistics.BOOLEAN_VALUE_BYTES;
import static java.util.Objects.requireNonNull;

public class BooleanStatisticsBuilder
        implements StatisticsBuilder
{
    private long nonNullValueCount;
    private long trueValueCount;

    public static BooleanStatisticsBuilder newBuilder() {
        return new BooleanStatisticsBuilder();
    }

    public void addValue(boolean value)
    {
        nonNullValueCount++;
        if (value) {
            trueValueCount++;
        }
    }

    private void addBooleanStatistics(long valueCount, BooleanStatistics value)
    {
        requireNonNull(value, "value is null");

        nonNullValueCount += valueCount;
        trueValueCount += value.getTrueValueCount();
    }

    private Optional<BooleanStatistics> buildBooleanStatistics()
    {
        if (nonNullValueCount == 0) {
            return Optional.empty();
        }
        return Optional.of(new BooleanStatistics(trueValueCount));
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        Optional<BooleanStatistics> booleanStatistics = buildBooleanStatistics();
        return new ColumnStatistics(
                nonNullValueCount,
                booleanStatistics.map(s -> BOOLEAN_VALUE_BYTES).orElse(0L),
                booleanStatistics.orElse(null),
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    @Override
    public ColumnStatistics buildColumnPartitionStatistics(String value) {
        try {
            SQLBoolean sqlBoolean = new SQLBoolean();
            sqlBoolean.setValue(value);
            addValue(sqlBoolean.getBoolean());
            return buildColumnStatistics();
        } catch (StandardException se) {
            throw new RuntimeException(se);
        }
    }

    public static Optional<BooleanStatistics> mergeBooleanStatistics(List<ColumnStatistics> stats)
    {
        BooleanStatisticsBuilder booleanStatisticsBuilder = new BooleanStatisticsBuilder();
        for (ColumnStatistics columnStatistics : stats) {
            BooleanStatistics partialStatistics = columnStatistics.getBooleanStatistics();
            if (columnStatistics.getNumberOfValues() > 0) {
                if (partialStatistics == null) {
                    // there are non null values but no statistics, so we can not say anything about the data
                    return Optional.empty();
                }
                booleanStatisticsBuilder.addBooleanStatistics(columnStatistics.getNumberOfValues(), partialStatistics);
            }
        }
        return booleanStatisticsBuilder.buildBooleanStatistics();
    }

}

