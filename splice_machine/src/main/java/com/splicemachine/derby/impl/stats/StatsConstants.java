package com.splicemachine.derby.impl.stats;

import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class StatsConstants extends SpliceConstants {
    static{
        setParameters(config);
    }

    public static int cardinalityPrecision;
    @Parameter public static final String CARDINALITY_PRECISION="splice.statistics.cardinality";
    @DefaultValue(value = CARDINALITY_PRECISION)public static final int DEFAULT_CARDINALITY_PRECISION=6;

    public static int topKSize;
    @Parameter public static final String TOPK_SIZE = "splice.statistics.topKSize";
    @DefaultValue(value = TOPK_SIZE)public static final int DEFAULT_TOPK_PRECISION = 5;

    public static void setParameters(Configuration config){
        cardinalityPrecision = config.getInt(CARDINALITY_PRECISION,DEFAULT_CARDINALITY_PRECISION);
        topKSize = config.getInt(TOPK_SIZE,DEFAULT_TOPK_PRECISION);
    }
}
