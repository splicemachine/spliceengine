package com.splicemachine.derby.utils;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.frequency.FrequentElements;

/**
 * @author Scott Fines
 *         Date: 3/2/15
 */
public class StatisticsFunctions {

    public static long CARDINALITY(ColumnStatistics columnStatistics){
        if(columnStatistics==null) return 0;
        return columnStatistics.cardinality();
    }

    public static long NULL_COUNT(ColumnStatistics columnStatistics){
        if(columnStatistics==null) return 0;
        return columnStatistics.nullCount();
    }

    public static float NULL_FRACTION(ColumnStatistics columnStatistics){
        if(columnStatistics==null) return 0;
        return columnStatistics.nullFraction();
    }

    public static String TOP_K(ColumnStatistics columnStatistics){
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

}
