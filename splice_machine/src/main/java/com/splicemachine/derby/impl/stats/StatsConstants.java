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

    public static int fetchSampleSize;
    @SpliceConstants.Parameter public static final String INDEX_FETCH_SAMPLE_SIZE = "splice.statistics.indexFetch.sampleSize";
    @DefaultValue(value = INDEX_FETCH_SAMPLE_SIZE) public static final int DEFAULT_INDEX_FETCH_SAMPLE_SIZE = 128;

    public static int fetchRepetitionCount;
    @SpliceConstants.Parameter public static final String INDEX_FETCH_REPETITION_COUNT = "splice.statistics.indexFetch.repetitionCount";
    @DefaultValue(value = INDEX_FETCH_REPETITION_COUNT) public static final int DEFAULT_INDEX_FETCH_REPETITION_COUNT = 3;


    /*
     * This is the cardinality fraction to fall back on when we cannot measure the cardinality directly (or
     * statistics are not available for some reason). By default it assumes that there are 100 duplicates
     * of every row (giving a cardinality fraction of 0.01)
     *
     * Note that this is configurable, but that configuring it is pretty pointless--it is FAR better to just
     * compute statistics on the column of interest to get more precise values. This is only here as an absolute
     * fallback when the algorithm can do nothing else.
     */
    public static double fallbackCardinalityFraction;
    @Parameter public static final String FALLBACK_CARDINALITY_FRACTION = "splice.statistics.defaultCardinalityFraction";
    @DefaultValue(value=FALLBACK_CARDINALITY_FRACTION)public static final double DEFAULT_FALLBACK_CARDINALITY_FRACTION=.1d;

    /**
     *  The fallback size of a region, when no statistics at all are present. Generally, it's much preferable
     *  to collect statistics than it is to fiddle with this setting--it's only here so that we have SOMETHING
     *  when no statistics are available.
     */
    public static long fallbackRegionRowCount;
    @Parameter public static final String FALLBACK_REGION_ROW_COUNT="splice.statistics.fallbackRegionRowCount";
    @DefaultValue(value = FALLBACK_REGION_ROW_COUNT) public static final long DEFAULT_FALLBACK_REGION_COUNT=5000000;

    /**
     * The fallback ratio of remote to local scan cost. This is only used when no statistics are present for the
     * table of interest; it is never a good idea to mess with this parameter--collect statistics instead.
     */
    public static long fallbackRemoteLatencyRatio;
    @Parameter public static final String FALLBACK_REMOTE_LATENCY_RATIO="splice.statistics.fallbackRemoteLatencyRatio";
    @DefaultValue(value =FALLBACK_REMOTE_LATENCY_RATIO) public static final long DEFAULT_FALLBACK_REMOTE_LATENCY_RATIO= 20l;

    /**
     * The fallback minimum number of rows to see when starts are not available. Rather than messing
     * around with this parameter, collect statistics instead. You'll be much better off.
     */
    public static long fallbackMinimumRowCount;
    @Parameter public static final String FALLBACK_MINIMUM_ROW_COUNT="splice.statistics.fallbackMinimumRowCount";
    @DefaultValue(value = FALLBACK_MINIMUM_ROW_COUNT) public static final long DEFAULT_FALLBACK_MINIMUM_ROW_COUNT=20;

    public static int fallbackRowWidth;
    @Parameter public static final String FALLBACK_ROW_WIDTH="splice.statistics.fallbackMinimumRowWidth";
    @DefaultValue(value = FALLBACK_ROW_WIDTH) public static final int DEFAULT_FALLBACK_ROW_WIDTH=170;

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

        int ifs = config.getInt(INDEX_FETCH_SAMPLE_SIZE,DEFAULT_INDEX_FETCH_SAMPLE_SIZE);
        int i = 8; //we want to fetch at least a FEW rows
        while(i<ifs)
            i<<=1;
        fetchSampleSize = i;

        fallbackRemoteLatencyRatio= config.getLong(FALLBACK_REMOTE_LATENCY_RATIO,DEFAULT_FALLBACK_REMOTE_LATENCY_RATIO);
        fallbackCardinalityFraction = config.getDouble(FALLBACK_CARDINALITY_FRACTION,DEFAULT_FALLBACK_CARDINALITY_FRACTION);
        fetchRepetitionCount = config.getInt(INDEX_FETCH_REPETITION_COUNT,DEFAULT_INDEX_FETCH_REPETITION_COUNT);

        fallbackRegionRowCount = config.getLong(FALLBACK_REGION_ROW_COUNT,DEFAULT_FALLBACK_REGION_COUNT);
        fallbackRowWidth = config.getInt(FALLBACK_ROW_WIDTH,DEFAULT_FALLBACK_ROW_WIDTH);
        fallbackMinimumRowCount = config.getLong(FALLBACK_MINIMUM_ROW_COUNT,DEFAULT_FALLBACK_MINIMUM_ROW_COUNT);
    }
}
