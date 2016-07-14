/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
