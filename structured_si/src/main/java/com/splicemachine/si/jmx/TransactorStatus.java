package com.splicemachine.si.jmx;

import javax.management.MXBean;

/**
 * Monitoring Hook for JMX.
 *
 * @author Scott Fines
 * Created on: 6/3/13
 */
@MXBean
public interface TransactorStatus {

    /**
     * @return the total number of child transactions created by this node.
     */
    long getTotalChildTransactions();


    //TODO -sf- support these last two methods
    /**
     * @return the total number of non-child transactions committed by this node.
     */
//    long getTotalCommittedChildTransactions();

    /**
     * @return the total number of non-child transactions rolled back by this node.
     */
//    long getTotalRolledBackChildTransactions();

    /**
     * @return the total number of non-child transactions created by this node.
     */
    long getTotalTransactions();

    /**
     * @return the total number of non-child transactions committed by this node.
     */
    long getTotalCommittedTransactions();

    /**
     * @return the total number of non-child transactions rolled back by this node.
     */
    long getTotalRolledBackTransactions();

    /**
     * @return the total number of failed transactions on this node.
     */
    long getTotalFailedTransactions();

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
}
