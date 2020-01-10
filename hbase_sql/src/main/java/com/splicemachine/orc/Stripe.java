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
package com.splicemachine.orc;

import com.splicemachine.orc.metadata.ColumnEncoding;
import com.splicemachine.orc.stream.StreamSources;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class Stripe
{
    private final long rowCount;
    private final List<ColumnEncoding> columnEncodings;
    private final List<RowGroup> rowGroups;
    private final StreamSources dictionaryStreamSources;

    public Stripe(long rowCount, List<ColumnEncoding> columnEncodings, List<RowGroup> rowGroups, StreamSources dictionaryStreamSources)
    {
        this.rowCount = rowCount;
        this.columnEncodings = requireNonNull(columnEncodings, "columnEncodings is null");
        this.rowGroups = ImmutableList.copyOf(requireNonNull(rowGroups, "rowGroups is null"));
        this.dictionaryStreamSources = requireNonNull(dictionaryStreamSources, "dictionaryStreamSources is null");
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public List<ColumnEncoding> getColumnEncodings()
    {
        return columnEncodings;
    }

    public List<RowGroup> getRowGroups()
    {
        return rowGroups;
    }

    public StreamSources getDictionaryStreamSources()
    {
        return dictionaryStreamSources;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("rowCount", rowCount)
                .add("columnEncodings", columnEncodings)
                .add("rowGroups", rowGroups)
                .add("dictionaryStreams", dictionaryStreamSources)
                .toString();
    }
}
