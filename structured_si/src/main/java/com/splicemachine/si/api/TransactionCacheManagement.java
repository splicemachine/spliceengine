package com.splicemachine.si.api;

import javax.management.MXBean;

/**
 * JMX Hook for monitoring the behavior of the Completed Transaction Cache.
 *
 * @author Scott Fines
 * Date: 7/1/14
 */
@MXBean
public interface TransactionCacheManagement {
		/**
		 * @return the total number (since the cache was created) of entries which
		 * were evicted since the cache was created
		 */
		long getTotalEvictedEntries();

		/**
		 * @return the total number (since the cache was created) of GET requests which
		 * could be served from cache.
		 */
		long getTotalHits();

		/**
		 * @return the total number (since the cache was created) of GET requests which
		 * could <em>not</em> be served from cache.
		 */
		long getTotalMisses();

		/**
		 * @return the total number of GET requests made against the transaction cache since
		 * it was created
		 */
		long getTotalRequests();

		/**
		 * @return the percentage of GET requests were hits--i.e. totalHits/totalRequests
		 */
		float getHitPercentage();

		/**
		 * @return an <em>estimate</em> of the total number of entries in the cache. This may over-estimate
		 * the cache size, since elements could be evicted due to memory pressure
		 */
		int getCurrentSize();

		/**
		 * @return the maximum number of entries which can be contained before an eviction is forced.
		 */
		int getMaxSize();
}
