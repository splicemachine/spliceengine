package com.splicemachine.si.api;

import javax.management.MXBean;

/**
 * @author Scott Fines
 * Created on: 6/3/13
 */
@MXBean
public interface TransactionStoreStatus {

    long getActiveTxnCacheHits();
    long getActiveTxnCacheMisses();
    double getActiveTxnCacheHitRatio();
    long getActiveTxnEvictionCount();

    long getImmutableTxnCacheHits();
    long getImmutableTxnCacheMisses();
    double getImmutableTxnCacheHitRatio();
    long getImmutableTxnEvictionCount();

    long getCacheHits();
    long getCacheMisses();
    double getCacheHitRatio();
    long getCacheEvictionCount();

    /**
     * @return the total number of Transactions which were loaded by the store
     */
    long getNumLoadedTxns();

    /**
     * @return the total number of Transaction updates which were written
     */
    long getNumTxnUpdatesWritten();

    int getCommitWaitTimeoutMs();

}
