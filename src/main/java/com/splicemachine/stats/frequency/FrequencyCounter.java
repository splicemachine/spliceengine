package com.splicemachine.stats.frequency;

import com.splicemachine.stats.Mergeable;
import com.splicemachine.stats.Updateable;

import java.util.Set;

/**
 * Represents an algorithm for computing a frequency distribution
 * of data (e.g. finding all elements in the set which occur
 * more than {@code s} times, for a user-specified {@code s}).
 *
 * @author Scott Fines
 * Date: 1/12/14
 */
public interface FrequencyCounter<T> extends Iterable<FrequencyEstimate<T>>,Updateable<T> {

		/**
		 * Get all elements whose frequency is at least {@code support * numElementsVisited},
		 * where {@code numElementsVisited} is the number of elements that the counter has seen
		 * in total.
		 *
		 * @param support the multiplicative factor for determining the threshold of estimation.
		 * @return All elements (within the accuracy guaranteed by the algorithm) which have a frequency
		 * of at least {support*numRowsVisited}
		 */
		Set<? extends FrequencyEstimate<T>> getFrequentElements(float support);

		/**
		 * Get the {@code k} most frequent elements. These are the elements which are more frequent than
		 * any others.
		 *
		 * <p>Note that the implementation is allowed to return fewer than {@code k } elements. This is because some
		 * implementations cannot guarantee {@code k} elements in all cases (depending on that instance's configuration).</p>
		 *
		 * @param k the maximum number of elements to return
		 * @return up to {@code k} elements whose frequency exceeds all others.
		 */
		Set<? extends FrequencyEstimate<T>> getMostFrequentElements(int k);
}
