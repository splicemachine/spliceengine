/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.si.jmx;

import javax.management.MXBean;

/**
 * Monitoring Hook for JMX.
 *
 * @author Scott Fines
 * Created on: 6/3/13
 */
@MXBean
@SuppressWarnings("unused")
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

    /**
     * @return the total number of Transactions which were loaded by the store
     */
    long getNumLoadedTxns();

    /**
     * @return the total number of Transaction updates which were written
     */
    long getNumTxnUpdatesWritten();
}
