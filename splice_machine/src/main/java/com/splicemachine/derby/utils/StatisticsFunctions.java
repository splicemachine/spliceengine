/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
