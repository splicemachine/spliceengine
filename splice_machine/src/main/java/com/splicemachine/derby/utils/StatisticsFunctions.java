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
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * @author Scott Fines
 *         Date: 3/2/15
 */
public class StatisticsFunctions {

    /*
    public static int STATS_COL_WIDTH(ColumnStatistics columnStatistics){
        if(columnStatistics==null) return 0;
        return columnStatistics.avgColumnWidth();
    }

    public static long STATS_CARDINALITY(ColumnStatistics columnStatistics){
        if(columnStatistics==null) return 0;
        return columnStatistics.cardinality();
    }

    public static long STATS_NULL_COUNT(ColumnStatistics columnStatistics){
        if(columnStatistics==null) return 0;
        return columnStatistics.nullCount();
    }

    public static float STATS_NULL_FRACTION(ColumnStatistics columnStatistics){
        if(columnStatistics==null) return 0;
        return columnStatistics.nullFraction();
    }

    public static String STATS_TOP_K(ColumnStatistics columnStatistics){
        if(columnStatistics==null) return null;
        FrequentElements frequentElements = columnStatistics.topK();
        StringBuilder string = new StringBuilder();
        boolean isFirst = true;
        for(Object estimate:frequentElements.allFrequentElements()){
            if(!isFirst) string = string.append(",");
            else isFirst = false;
            string = string.append(estimate);
        }
        return string.toString();
    }

    public static String STATS_MAX(ColumnStatistics columnStatistics) throws StandardException {
        if(columnStatistics==null) return null;
        DataValueDescriptor dvd = (DataValueDescriptor)columnStatistics.maxValue();
        if(dvd==null || dvd.isNull()) return null;
        return dvd.getString();
    }

    public static String STATS_MIN(ColumnStatistics columnStatistics) throws StandardException {
        if(columnStatistics==null) return null;
        DataValueDescriptor dvd = (DataValueDescriptor)columnStatistics.minValue();
        if(dvd==null || dvd.isNull()) return null;
        return dvd.getString();
    }

    */
}
