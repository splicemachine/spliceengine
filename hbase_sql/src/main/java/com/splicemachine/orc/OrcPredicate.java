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

import com.splicemachine.orc.metadata.ColumnStatistics;

import java.util.Map;

public interface OrcPredicate
{
    OrcPredicate TRUE = new OrcPredicate()
    {
        @Override
        public boolean matches(long numberOfRows, Map<Integer, ColumnStatistics> statisticsByColumnIndex)
        {
            return true;
        }
    };

    /**
     * Should the ORC reader process a file section with the specified statistics.
     *
     * @param numberOfRows the number of rows in the segment; this can be used with
     * {@code ColumnStatistics} to determine if a column is only null
     * @param statisticsByColumnIndex statistics for column by ordinal position
     * in the file; this will match the field order from the hive metastore
     */
    boolean matches(long numberOfRows, Map<Integer, ColumnStatistics> statisticsByColumnIndex);
}
