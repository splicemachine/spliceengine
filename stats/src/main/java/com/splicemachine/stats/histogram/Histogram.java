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

package com.splicemachine.stats.histogram;

import java.util.List;

/**
 * Represents a histogram--an ordered estimation of the distribution of the data.
 *
 * @author Scott Fines
 * Date: 3/31/14
 */
public interface Histogram<T extends Comparable<T>> extends RangeQuerySolver<T> {
		/**
		 * A Read-only view into the internal structure of the Histogram
		 */
		static interface Column<T extends Comparable<T>> extends Comparable<Column<T>>{
				/**
				 * @return the left-most element in the Column.
				 */
				T getLeastElement();

				/**
				 * @return the total number of elements in the column.
				 */
				long getNumElements();

				long estimateLessThan(T element,boolean inclusive);

				long estimateGreaterThan(T element, boolean inclusive);

				long estimateEquals(T element);

				Column<T> lowInterpolatedColumn(T element);

				Column<T> highInterpolatedColumn(T element);
		}

		/**
		 * @return a read-only view of the internal columns.
		 * Modification of this list is either not allowed, or will
		 * <em>not</em> be reflected in the structure of the histogram. Basically,
		 * just read it, don't screw with it, or you might violate the invariants
		 * of the underlying implementation.
		 */
		List<? extends Column<T>> getColumns();

		/**
		 * @return the number of buckets in the histogram
		 */
		int getNumBuckets();

		/**
		 * Returns an estimate of the maximum error made when computing the estimated number of elements.
		 * Some implementations may not provide this, in which case an {@link java.lang.UnsupportedOperationException} will be
		 * thrown.
		 *
		 * @param start the start of the range of interest, or {@code null} if the estimate
		 *                should be from the min element of the histogram.
		 * @param end the end of the range of interest, or {@code null} if the estimate should be
		 *              to the maximum element of the histogram.
		 * @return the (estimated) maximum error made when estimating the number of elements in the specified range.
		 * @throws java.lang.IllegalArgumentException if {@code start.equals(end)}, and the underlying structure
		 * 																						is unable to estimate that quantity
		 * @throws java.lang.UnsupportedOperationException if the underlying algorithm is unable to estimate its own error.
		 */
		long maxError(T start, T end,boolean inclusiveStart, boolean inclusiveEnd);

}
