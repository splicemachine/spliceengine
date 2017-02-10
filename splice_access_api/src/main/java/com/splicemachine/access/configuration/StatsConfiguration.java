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

package com.splicemachine.access.configuration;

/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public class StatsConfiguration implements ConfigurationDefault {

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

    @Override
    public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        // FIXME: JC - some of these are not referenced anywhere outside. Do we need them?

        builder.fallbackRowWidth = configurationSource.getInt(FALLBACK_ROW_WIDTH, DEFAULT_FALLBACK_ROW_WIDTH);
//        builder.indexFetchRepititionCount = configurationSource.getInt(INDEX_FETCH_REPETITION_COUNT, DEFAULT_INDEX_FETCH_REPETITION_COUNT);
        builder.indexFetchSampleSize = configurationSource.getInt(INDEX_FETCH_SAMPLE_SIZE, DEFAULT_INDEX_FETCH_SAMPLE_SIZE);
        builder.topkSize = configurationSource.getInt(TOPK_SIZE, DEFAULT_TOPK_PRECISION);
        builder.cardinalityPrecision = configurationSource.getInt(CARDINALITY_PRECISION, DEFAULT_CARDINALITY_PRECISION);

        builder.fallbackMinimumRowCount = configurationSource.getLong(FALLBACK_MINIMUM_ROW_COUNT, DEFAULT_FALLBACK_MINIMUM_ROW_COUNT);
        builder.fallbackOpencloseLatency = configurationSource.getLong(FALLBACK_OPENCLOSE_LATENCY, DEFAULT_FALLBACK_OPENCLOSE_LATENCY);
        builder.fallbackLocalLatency = configurationSource.getLong(FALLBACK_LOCAL_LATENCY, DEFAULT_FALLBACK_LOCAL_LATENCY);
        builder.fallbackRemoteLatencyRatio = configurationSource.getLong(FALLBACK_REMOTE_LATENCY_RATIO, DEFAULT_FALLBACK_REMOTE_LATENCY_RATIO);
        builder.partitionCacheExpiration = configurationSource.getLong(PARTITION_CACHE_EXPIRATION, DEFAULT_PARTITION_CACHE_EXPIRATION);
//        builder.partitionCacheSize = configurationSource.getLong(PARTITION_CACHE_SIZE, DEFAULT_PARTITION_CACHE_SIZE);
        builder.fallbackRegionRowCount = configurationSource.getLong(FALLBACK_REGION_ROW_COUNT, DEFAULT_FALLBACK_REGION_COUNT);

        builder.fallbackNullFraction = configurationSource.getDouble(FALLBACK_NULL_FRACTION, DEFAULT_FALLBACK_NULL_FRACTION);
//        builder.fallbackCardinalityFraction = configurationSource.getDouble(FALLBACK_CARDINALITY_FRACTION, DEFAULT_FALLBACK_CARDINALITY_FRACTION);
//        builder.fallbackIndexSelectivityFraction = configurationSource.getDouble(FALLBACK_INDEX_SELECTIVITY_FRACTION, DEFAULT_FALLBACK_INDEX_SELECTIVITY_FRACTION);
        builder.optimizerExtraQualifierMultiplier = configurationSource.getDouble(OPTIMIZER_EXTRA_QUALIFIER_MULTIPLIER, DEFAULT_OPTIMIZER_EXTRA_QUALIFIER_MULTIPLIER);
    }
}
