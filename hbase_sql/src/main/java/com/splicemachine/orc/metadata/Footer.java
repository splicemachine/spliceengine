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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.Maps.transformValues;
import static java.util.Objects.requireNonNull;

public class Footer
{
    private final long numberOfRows;
    private final int rowsInRowGroup;
    private final List<StripeInformation> stripes;
    private final List<OrcType> types;
    private final List<ColumnStatistics> fileStats;
    private final Map<String, Slice> userMetadata;

    public Footer(long numberOfRows, int rowsInRowGroup, List<StripeInformation> stripes, List<OrcType> types, List<ColumnStatistics> fileStats, Map<String, Slice> userMetadata)
    {
        this.numberOfRows = numberOfRows;
        this.rowsInRowGroup = rowsInRowGroup;
        this.stripes = ImmutableList.copyOf(requireNonNull(stripes, "stripes is null"));
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.fileStats = ImmutableList.copyOf(requireNonNull(fileStats, "columnStatistics is null"));
        requireNonNull(userMetadata, "userMetadata is null");
        this.userMetadata = ImmutableMap.copyOf(transformValues(userMetadata, Slices::copyOf));
    }

    public long getNumberOfRows()
    {
        return numberOfRows;
    }

    public int getRowsInRowGroup()
    {
        return rowsInRowGroup;
    }

    public List<StripeInformation> getStripes()
    {
        return stripes;
    }

    public List<OrcType> getTypes()
    {
        return types;
    }

    public List<ColumnStatistics> getFileStats()
    {
        return fileStats;
    }

    public Map<String, Slice> getUserMetadata()
    {
        return ImmutableMap.copyOf(transformValues(userMetadata, Slices::copyOf));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRows", numberOfRows)
                .add("rowsInRowGroup", rowsInRowGroup)
                .add("stripes", stripes)
                .add("types", types)
                .add("columnStatistics", fileStats)
                .add("userMetadata", userMetadata.keySet())
                .toString();
    }
}
