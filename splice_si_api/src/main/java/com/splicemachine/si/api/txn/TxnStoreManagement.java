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

package com.splicemachine.si.api.txn;

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
