/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.db.impl.sql.compile;

public class FirstColumnOfIndexStats {
    // Number of rows in the conglomerate over number of values in the first column.
    private double firstIndexColumnRowsPerValue = -1d;

    // Number of rows in the table.
    private long rowCountFromStats = -1L;

    // Number of values in the first column of the conglomerate according to stats.
    private long firstIndexColumnCardinality = -1L;

    public FirstColumnOfIndexStats() { }

    public FirstColumnOfIndexStats(FirstColumnOfIndexStats other) {
        if (other != null) {
            firstIndexColumnRowsPerValue = other.getFirstIndexColumnRowsPerValue();
            rowCountFromStats = other.getRowCountFromStats();
            firstIndexColumnCardinality = other.getFirstIndexColumnCardinality();
        }
    }

    public void reset() {
        firstIndexColumnRowsPerValue = -1d;
        rowCountFromStats = -1L;
        firstIndexColumnCardinality = -1L;
    }

    public double getFirstIndexColumnRowsPerValue() {
        return firstIndexColumnRowsPerValue;
    }

    public void setFirstIndexColumnRowsPerValue(double firstIndexColumnRowsPerValue) {
        this.firstIndexColumnRowsPerValue = firstIndexColumnRowsPerValue;
    }

    public long getRowCountFromStats() {
        return rowCountFromStats;
    }

    public void setRowCountFromStats(long rowCountFromStats) {
        this.rowCountFromStats = rowCountFromStats;
    }

    public long getFirstIndexColumnCardinality() {
        return firstIndexColumnCardinality;
    }

    public void setFirstIndexColumnCardinality(long firstIndexColumnCardinality) {
        this.firstIndexColumnCardinality = firstIndexColumnCardinality;
    }
}
