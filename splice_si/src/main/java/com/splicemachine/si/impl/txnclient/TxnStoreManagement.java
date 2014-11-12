package com.splicemachine.si.impl.txnclient;

import javax.management.MXBean;

/**
 * @author Scott Fines
 * Date: 9/8/14
 */
@MXBean
public interface TxnStoreManagement {

    long getTotalTxnLookups();

    long getTotalTxnElevations();

    long getTotalWritableTxnsCreated();

    long getTotalRollbacks();

    long getTotalCommits();

    /**
     * @return the total number (since the cache was created) of entries which
     * were evicted since the cache was created
     */
    long getTotalEvictedCacheEntries();

    /**
     * @return the total number (since the cache was created) of GET requests which
     * could be served from cache.
     */
    long getTotalCacheHits();

    /**
     * @return the total number (since the cache was created) of GET requests which
     * could <em>not</em> be served from cache.
     */
    long getTotalCacheMisses();

    /**
     * @return the total number of GET requests made against the transaction cache since
     * it was created
     */
    long getTotalCacheRequests();

    /**
     * @return the percentage of GET requests were hits--i.e. totalHits/totalRequests
     */
    float getCacheHitPercentage();

    /**
     * @return an <em>estimate</em> of the total number of entries in the cache. This may over-estimate
     * the cache size, since elements could be evicted due to memory pressure
     */
    int getCurrentCacheSize();

    /**
     * @return the maximum number of entries which can be contained before an eviction is forced.
     */
    int getMaxCacheSize();
}
