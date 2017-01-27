/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 *         Date: 3/2/15
 */
public class StatisticsFunctions {

    public static int STATS_COL_WIDTH(ColumnStatisticsImpl itemStatistics) throws StandardException {
            if (itemStatistics == null) return 0;
            return itemStatistics.getColumnDescriptor().getLength(); // TODO JL
    }

    public static long STATS_CARDINALITY(ColumnStatisticsImpl itemStatistics){
        if(itemStatistics==null) return 0;
        return itemStatistics.cardinality();
    }

    public static long STATS_NULL_COUNT(ColumnStatisticsImpl itemStatistics){
        if(itemStatistics==null) return 0;
        return itemStatistics.nullCount();
    }

    public static float STATS_NULL_FRACTION(ColumnStatisticsImpl itemStatistics){
        if(itemStatistics==null) return 0;
        if (itemStatistics.notNullCount() + itemStatistics.nullCount() == 0) // Divide by null check
            return 0;
        return ((float) itemStatistics.nullCount()/ (float) (itemStatistics.notNullCount()+itemStatistics.nullCount()));
    }

    public static String STATS_MAX(ColumnStatisticsImpl itemStatistics) throws StandardException {
        if(itemStatistics==null) return null;
        DataValueDescriptor dvd = (DataValueDescriptor)itemStatistics.maxValue();
        if(dvd==null || dvd.isNull()) return null;
        return dvd.getString();
    }

    public static String STATS_MIN(ColumnStatisticsImpl itemStatistics) throws StandardException {
        if(itemStatistics==null) return null;
        DataValueDescriptor dvd = (DataValueDescriptor)itemStatistics.minValue();
        if(dvd==null || dvd.isNull()) return null;
        return dvd.getString();
    }

    public static String STATS_QUANTILES(ColumnStatisticsImpl itemStatistics) throws StandardException {
        if(itemStatistics==null)
            return null;
        return itemStatistics.getQuantilesSketch().toString(true,true);
    }

    public static String STATS_FREQUENCIES(ColumnStatisticsImpl itemStatistics) throws StandardException {
        if(itemStatistics==null)
            return null;
        return itemStatistics.getFrequenciesSketch().toString();
    }

    public static String STATS_THETA(ColumnStatisticsImpl itemStatistics) throws StandardException {
        if(itemStatistics==null)
            return null;
        return itemStatistics.getThetaSketch().toString();
    }

}
