package com.splicemachine.derby.impl.stats;

import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class StatsConstants extends SpliceConstants {
    private static final Logger LOG = Logger.getLogger(StatsConstants.class);
    static{
        setParameters(config);
    }


    /*
     * The default precision to use when estimating the cardinality of a given column in a partition.
     * This number can be chosen to be anything >=4, but the total memory used per column will
     * change as mem_used = (1<<CARDINALITY_PRECISION) bytes, so don't make this too high, or you
     * will end up using a lot more memory than you would otherwise wish.
     */
    public static int cardinalityPrecision;
    @Parameter public static final String CARDINALITY_PRECISION="splice.statistics.cardinality";
    @DefaultValue(value = CARDINALITY_PRECISION)public static final int DEFAULT_CARDINALITY_PRECISION=8;

    /*
     * The number of "top-k" frequent elements to keep for each collection on each column in a partition.
     */
    public static int topKSize;
    @Parameter public static final String TOPK_SIZE = "splice.statistics.topKSize";
    @DefaultValue(value = TOPK_SIZE)public static final int DEFAULT_TOPK_PRECISION = 5;

    /*
     * The size of the partition statistics cache. Partitions will be evicted when the total size
     * exceeds this threshold
     */
    public static long partitionCacheSize;
    @Parameter public static final String PARTITION_CACHE_SIZE = "splice.statistics.partitionCache.size";
    @DefaultValue(value = PARTITION_CACHE_SIZE) public static final long DEFAULT_PARTITION_CACHE_SIZE = 8192;
    /*
     * The amount of time to keep a given partition in the partition cache. Turning this number up will
     * decrease the amount of network calls made to fetch statistics, but will also allow statistics
     * to grow more stale before being replaced. Tune this with caution. Measured in Milliseconds
     *
     */
    public static long partitionCacheExpiration;
    @Parameter public static final String PARTITION_CACHE_EXPIRATION = "splice.statistics.partitionCache.expiration";
    @DefaultValue(value = PARTITION_CACHE_EXPIRATION) public static final long DEFAULT_PARTITION_CACHE_EXPIRATION = 60*1000; //1 minute by default

    public static void setParameters(Configuration config){
        int cp = config.getInt(CARDINALITY_PRECISION,DEFAULT_CARDINALITY_PRECISION);
        if(cp <4) {
            LOG.warn("Cardinality Precision is set too low, adjusting to minimum setting of 4");
            cp = 4;
        }
        cardinalityPrecision = cp;
        topKSize = config.getInt(TOPK_SIZE,DEFAULT_TOPK_PRECISION);
        partitionCacheSize = config.getLong(PARTITION_CACHE_SIZE,DEFAULT_PARTITION_CACHE_SIZE);
        partitionCacheExpiration = config.getLong(PARTITION_CACHE_EXPIRATION,DEFAULT_PARTITION_CACHE_EXPIRATION);
    }
}
