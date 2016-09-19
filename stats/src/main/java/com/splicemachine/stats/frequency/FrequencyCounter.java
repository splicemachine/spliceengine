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

package com.splicemachine.stats.frequency;

import com.splicemachine.stats.Updateable;

import java.util.Set;

/**
 * Represents an algorithm for computing a frequency distribution
 * of data (e.g. finding all elements in the set which occur
 * more than {@code s} times, for a user-specified {@code s}).
 *
 * For more information on the Frequent elements problem, see the package javadoc.
 *
 * Implementations are not required to be thread-safe (although they are allowed to be if they choose).
 *
 * @author Scott Fines
 * Date: 1/12/14
 */
public interface FrequencyCounter<T> extends Updateable<T> {

    /**
     * Get all elements which have occurred more than {@code support*numVisited} times,
     * where {@code numVisitied} is the total number of elements that the counter has seen
     * in total. For example, to find all elements which occupy more than 10% of the table, we
     * would call {@code heavyHitters(0.1)}
     *
     * @param support the percentage of the table which is occupied by the defined "heavy hitter"
     * @return All elements (within the accuracy guaranteed by the algorithm) which have a frequency
     * of at least {@code support*numVisited}
     */
    FrequentElements<T> heavyHitters(float support);

    /**
     * Find the {@code k} most frequently seen elements. These ae
     *
     * <p>Note that the implementation is allowed to return fewer than {@code k} elements. This
     * is because some implementations cannot guarantee {@code k} elements in all cases (depending
     * on the configuration and the guarantees provided by that algorithm. No algorithm will ever
     * return <em>more</em> than {@code k} elements, however.
     *
     * @param k the maximum number of elements to return
     * @return up to {@code k} elements whose frequency exceeds all others.
     */
    FrequentElements<T> frequentElements(int k);
}
