package com.splicemachine.derby.impl.stats;

import com.splicemachine.access.api.SConfiguration;

/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public class StatsConfiguration{

    /*
     * The default precision to use when estimating the cardinality of a given column in a partition.
     * This number can be chosen to be anything >=4, but the total memory used per column will
     * change as mem_used = (1<<CARDINALITY_PRECISION) bytes, so don't make this too high, or you
     * will end up using a lot more memory than you would otherwise wish.
     */
    public static final String CARDINALITY_PRECISION="splice.statistics.cardinality";
    public static final int DEFAULT_CARDINALITY_PRECISION=14;

    /*
     * The number of "top-k" frequent elements to keep for each collection on each column in a partition.
     */
    public static final String TOPK_SIZE = "splice.statistics.topKSize";
    public static final int DEFAULT_TOPK_PRECISION = 10;

    /*
     * The size of the partition statistics cache. Partitions will be evicted when the total size
     * exceeds this threshold
     */
    public static final String PARTITION_CACHE_SIZE = "splice.statistics.partitionCache.size";
    public static final long DEFAULT_PARTITION_CACHE_SIZE = 8192;
    /*
     * The amount of time to keep a given partition in the partition cache. Turning this number up will
     * decrease the amount of network calls made to fetch statistics, but will also allow statistics
     * to grow more stale before being replaced. Tune this with caution. Measured in Milliseconds
     *
     */
    public static final String PARTITION_CACHE_EXPIRATION = "splice.statistics.partitionCache.expiration";
    public static final long DEFAULT_PARTITION_CACHE_EXPIRATION = 60*1000; //1 minute by default

    public static final String INDEX_FETCH_SAMPLE_SIZE = "splice.statistics.indexFetch.sampleSize";
    public static final int DEFAULT_INDEX_FETCH_SAMPLE_SIZE = 128;

    public static final String INDEX_FETCH_REPETITION_COUNT = "splice.statistics.indexFetch.repetitionCount";
    public static final int DEFAULT_INDEX_FETCH_REPETITION_COUNT = 3;


    /*
     * This is the cardinality fraction to fall back on when we cannot measure the cardinality directly (or
     * statistics are not available for some reason). By default it assumes that there are 100 duplicates
     * of every row (giving a cardinality fraction of 0.01)
     *
     * Note that this is configurable, but that configuring it is pretty pointless--it is FAR better to just
     * compute statistics on the column of interest to get more precise values. This is only here as an absolute
     * fallback when the algorithm can do nothing else.
     */
    /**
     *
     */
    public static final String FALLBACK_CARDINALITY_FRACTION = "splice.statistics.defaultCardinalityFraction";
    public static final double DEFAULT_FALLBACK_CARDINALITY_FRACTION=.1d;

    public static final String FALLBACK_NULL_FRACTION = "splice.statistics.defaultNullFraction";
    public static final double DEFAULT_FALLBACK_NULL_FRACTION=.1d;

    public static final String FALLBACK_INDEX_SELECTIVITY_FRACTION = "splice.statistics.defaultIndexSelectivityFraction";
    public static final double DEFAULT_FALLBACK_INDEX_SELECTIVITY_FRACTION=.1d;

    /**
     *
     * This metric is multiplied by number of rows and cost to determine an effect of 1..n extra qualifiers on the source result set.
     *
     */
    public static final String OPTIMIZER_EXTRA_QUALIFIER_MULTIPLIER = "splice.optimizer.extraQualifierMultiplier";
    private static final double DEFAULT_OPTIMIZER_EXTRA_QUALIFIER_MULTIPLIER = 0.9d;

    /**
     *  The fallback size of a region, when no statistics at all are present. Generally, it's much preferable
     *  to collect statistics than it is to fiddle with this setting--it's only here so that we have SOMETHING
     *  when no statistics are available.
     */
    public static final String FALLBACK_REGION_ROW_COUNT="splice.statistics.fallbackRegionRowCount";
    public static final long DEFAULT_FALLBACK_REGION_COUNT=5000000;

    /**
     * The fallback ratio of remote to local scan cost. This is only used when no statistics are present for the
     * table of interest; it is never a good idea to mess with this parameter--collect statistics instead.
     */
    public static final String FALLBACK_REMOTE_LATENCY_RATIO="splice.statistics.fallbackRemoteLatencyRatio";
    public static final long DEFAULT_FALLBACK_REMOTE_LATENCY_RATIO= 10l;

    public static final String FALLBACK_LOCAL_LATENCY="splice.statistics.fallbackLocalLatency";
    public static final long DEFAULT_FALLBACK_LOCAL_LATENCY= 1l;

    /**
     *
     * Open Close Latency in Microseconds
     *
     * 2 ms default
     */
    public static final String FALLBACK_OPENCLOSE_LATENCY="splice.statistics.fallbackOpenCloseLatency";
    public static final long DEFAULT_FALLBACK_OPENCLOSE_LATENCY= 2*1000;


    /**
     * The fallback minimum number of rows to see when starts are not available. Rather than messing
     * around with this parameter, collect statistics instead. You'll be much better off.
     */
    public static final String FALLBACK_MINIMUM_ROW_COUNT="splice.statistics.fallbackMinimumRowCount";
    public static final long DEFAULT_FALLBACK_MINIMUM_ROW_COUNT=20;

    public static final String FALLBACK_ROW_WIDTH="splice.statistics.fallbackMinimumRowWidth";
    public static final int DEFAULT_FALLBACK_ROW_WIDTH=170;

    public static final SConfiguration.Defaults defaults = new SConfiguration.Defaults(){

        @Override
        public boolean hasLongDefault(String key){
            switch(key){
                case FALLBACK_MINIMUM_ROW_COUNT:
                case FALLBACK_OPENCLOSE_LATENCY:
                case FALLBACK_LOCAL_LATENCY:
                case FALLBACK_REMOTE_LATENCY_RATIO:
                case PARTITION_CACHE_EXPIRATION:
                case PARTITION_CACHE_SIZE:
                case FALLBACK_REGION_ROW_COUNT:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public long defaultLongFor(String key){
            switch(key){
                case FALLBACK_MINIMUM_ROW_COUNT: return DEFAULT_FALLBACK_MINIMUM_ROW_COUNT;
                case FALLBACK_OPENCLOSE_LATENCY: return DEFAULT_FALLBACK_OPENCLOSE_LATENCY;
                case FALLBACK_LOCAL_LATENCY: return DEFAULT_FALLBACK_LOCAL_LATENCY;
                case FALLBACK_REMOTE_LATENCY_RATIO: return DEFAULT_FALLBACK_REMOTE_LATENCY_RATIO;
                case PARTITION_CACHE_EXPIRATION: return DEFAULT_PARTITION_CACHE_EXPIRATION;
                case PARTITION_CACHE_SIZE: return DEFAULT_PARTITION_CACHE_SIZE;
                case FALLBACK_REGION_ROW_COUNT: return DEFAULT_FALLBACK_REGION_COUNT;
                default:
                    throw new IllegalStateException("No Stats default found for key '"+key+"'");
            }
        }

        @Override
        public boolean hasIntDefault(String key){
            switch(key){
                case FALLBACK_ROW_WIDTH:
                case INDEX_FETCH_REPETITION_COUNT:
                case INDEX_FETCH_SAMPLE_SIZE:
                case TOPK_SIZE:
                case CARDINALITY_PRECISION:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public int defaultIntFor(String key){
            switch(key){
                case FALLBACK_ROW_WIDTH: return DEFAULT_FALLBACK_ROW_WIDTH;
                case INDEX_FETCH_REPETITION_COUNT: return DEFAULT_INDEX_FETCH_REPETITION_COUNT;
                case INDEX_FETCH_SAMPLE_SIZE: return DEFAULT_INDEX_FETCH_SAMPLE_SIZE;
                case TOPK_SIZE: return DEFAULT_TOPK_PRECISION;
                case CARDINALITY_PRECISION: return DEFAULT_CARDINALITY_PRECISION;
                default:
                    throw new IllegalStateException("No Stats default found for key '"+key+"'");
            }
        }

        @Override
        public boolean hasStringDefault(String key){
            return false;
        }

        @Override
        public String defaultStringFor(String key){
            return null;
        }

        @Override
        public boolean defaultBooleanFor(String key){
            throw new IllegalStateException("No Stats default found for key '"+key+"'");
        }

        @Override
        public boolean hasBooleanDefault(String key){
            return false;
        }

        @Override
        public double defaultDoubleFor(String key){
            switch(key){
                case FALLBACK_NULL_FRACTION: return DEFAULT_FALLBACK_NULL_FRACTION;
                case FALLBACK_CARDINALITY_FRACTION: return DEFAULT_FALLBACK_CARDINALITY_FRACTION;
                case FALLBACK_INDEX_SELECTIVITY_FRACTION: return DEFAULT_FALLBACK_INDEX_SELECTIVITY_FRACTION;
                case OPTIMIZER_EXTRA_QUALIFIER_MULTIPLIER: return DEFAULT_OPTIMIZER_EXTRA_QUALIFIER_MULTIPLIER;
                default:
                    throw new IllegalStateException("No Stats default found for key '"+key+"'");
            }
        }

        @Override
        public boolean hasDoubleDefault(String key){
            switch(key){
                case FALLBACK_NULL_FRACTION:
                case FALLBACK_CARDINALITY_FRACTION:
                case FALLBACK_INDEX_SELECTIVITY_FRACTION:
                case OPTIMIZER_EXTRA_QUALIFIER_MULTIPLIER:
                    return true;
                default:
                    throw new IllegalStateException("No Stats default found for key '"+key+"'");
            }
        }
    };
}
