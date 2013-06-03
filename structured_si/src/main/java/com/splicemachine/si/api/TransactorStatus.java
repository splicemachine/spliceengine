package com.splicemachine.si.api;

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
}
