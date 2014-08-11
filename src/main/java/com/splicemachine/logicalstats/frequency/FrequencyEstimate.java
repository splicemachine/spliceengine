package com.splicemachine.logicalstats.frequency;

/**
 * Represents an Estimate of the frequency of the specific element.
 *
 * @author Scott Fines
 * Date: 3/25/14
 */
public interface FrequencyEstimate<T> {

		/**
		 * @return the value being estimated
		 */
		T value();

		/**
		 * @return the (potentially over-)estimated frequency count
		 */
		long count();

		/**
		 * @return an estimate of how much the estimate overcounted. The <em>guaranteed count</em> is
		 * equivalent to {@link #count()}-{@code error()}
		 */
		long error();
}