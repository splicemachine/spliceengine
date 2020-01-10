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

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class RowGroupIndex
{
    private final List<Integer> positions;
    private final ColumnStatistics statistics;

    public RowGroupIndex(List<Integer> positions, ColumnStatistics statistics)
    {
        this.positions = ImmutableList.copyOf(requireNonNull(positions, "positions is null"));
        this.statistics = requireNonNull(statistics, "statistics is null");
    }

    public List<Integer> getPositions()
    {
        return positions;
    }

    public ColumnStatistics getColumnStatistics()
    {
        return statistics;
    }
}
