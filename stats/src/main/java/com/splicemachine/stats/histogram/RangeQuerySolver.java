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

/**
 * @author Scott Fines
 *         Date: 4/30/14
 */
public interface RangeQuerySolver<T extends Comparable<T>> {
		/**
		 * Get the (estimated) number of elements which are contained
		 * within the specified bounds. {@code null} is treated as "from the very beginning.
		 *
		 *
		 * @param start the start of the range of interest, or {@code null} if the estimate
		 *              should be from the min element of the histogram.
		 * @param end the end of the range of interest, or {@code null} if the estimate should be
		 *            to the maximum element of the histogram.
		 * @param inclusiveStart whether or not to include an estimate of equality on the start key (e.g. if
		 *                       set to true, will do >= start. If false, will do > start)
		 * @param inclusiveEnd whether or not to include an estimate of equality on the end key (e.g. if set to true,
		 *                     will do <= end. If false, will do < end).
		 * @return the (estimated) number of elements in the specified range.
		 * @throws java.lang.IllegalArgumentException if {@code start.equals(end)}, and the underlying structure is
		 * 																						unable to estimate that quantity
		 */
		long getNumElements(T start, T end,boolean inclusiveStart,boolean inclusiveEnd);

		/**
		 * @return the minimum element in the histogram.
		 */
		T getMin();

		/**
		 * @return the maximum element in the histogram.
		 */
		T getMax();
}
