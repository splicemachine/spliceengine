package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.frequency.FrequentElements;

/**
 * @author Scott Fines
 *         Date: 3/2/15
 */
public class StatisticsFunctions {

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

}
