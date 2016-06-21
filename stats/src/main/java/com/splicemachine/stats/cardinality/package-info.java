/**
 * This package contains logic and code around computing <em>Cardinality Estimates</em>
 * for various data sets and data types.
 *
 * For any data set {@code X} which allows duplicate elements, the <em>cardinality</em> of that data set
 * is the number of <em>distinct</em> elements contained in the data set.
 *
 * Computing the exact cardinality of a data set of size {@code N} requires {@code O(N)} space, which is
 * generally prohibitive for streaming applications. As a result, we look for algorithms which compute
 * an approximate solution.
 *
 * <h1>HyperLogLog</h1>
 *
 * There are a number of algorithms for computing the approximate cardinality of a data set, but the most popular
 * is the <em>HyperLogLog</em> algorithm devised by Flajolet et. al
 * ( "<em>HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm</em>"). This algorithm
 * uses a fixed number of counters, which allows a tradeoff between memory and accuracy--the more accuracy
 * which is desired, the more memory is needed.
 *
 * However, HyperLogLog does exhibit a tendency to overestimate cardinalities when the data set has very
 * few distinct elements. Heule et al adjusted this bias empirically
 * ("<em>HyperLogLog in Practice: Algorithmic Engineering of a State of the Art Cardinality Estimation Algorithm</em>")
 * to deal with low-cardinality estimates, and simultaneously found ways to adjust the overall memory usage as well.
 * This approach is the one which is adopted in most implementations here.
 *
 * Of course, when the data type guarantees a maximum cardinality which is small enough to fit within a small,
 * exact data structure(booleans, and a single byte), we simply compute the value directly.
 *
 * The main interface of interest here is the {@link com.splicemachine.stats.cardinality.CardinalityEstimator},
 * which can be constructed using the {@link com.splicemachine.stats.cardinality.CardinalityEstimators}
 * factory classes. Since both HyperLogLog and direct computation algorithms are linear, these
 * interfaces are {@link com.splicemachine.stats.Mergeable} as well.
 *
 * @author Scott Fines
 * Date: 2/25/15
 */
package com.splicemachine.stats.cardinality;