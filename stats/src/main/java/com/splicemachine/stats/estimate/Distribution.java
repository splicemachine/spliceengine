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

package com.splicemachine.stats.estimate;

/**
 * Utility for estimating the size of a query.
 *
 * In particular, this class is responsible for answering the following question:
 *
 * <ul>
 *     <li>The "latency" to access a single record of type in the dataset</li>
 *     <li>The number of rows in the query</li>
 *     <li>The "latency" in accessing all the rows in the query</li>
 * </ul>
 *
 * This is a central component for estimating query size with returned Table and Column Statistics.
 *
 * @author Scott Fines
 *         Date: 3/4/15
 */
public interface Distribution<T> {

    T minValue();

    long minCount();

    T maxValue();

    long totalCount();

    /**
     * @param element the element to match
     * @return the number of entries which are <em>equal</em> to the specified element.
     */
    long selectivity(T element);

    /**
     * @param start the start of the range to estimate. If {@code null}, then scan everything before {@code stop}.
     *              If {@code stop} is also {@code null}, then this will return an estimate to the number of entries
     *              in the entire data set.
     * @param stop the end of the range to estimate. If {@code null}, then scan everything after {@code start}.
     *             If {@code start} is also {@code null}, then this will return an estimate of the number of entries
     *             in the entire data set.
     * @param includeStart if {@code true}, then include entries which are equal to {@code start}
     * @param includeStop if {@code true}, then include entries which are <em>equal</em> to {@code stop}
     * @return the number of rows which fall in the range {@code start},{@code stop}, with
     * inclusion determined by {@code includeStart} and {@code includeStop}
     */
    long rangeSelectivity(T start,T stop, boolean includeStart,boolean includeStop);

}
