/**
 * This package contains logic and code around approximate and precise <em>Frequent Elements</em> and
 * <em>Heavy Hitter</em> algorithms.
 *
 * <em>Frequent Elements</em> are defined loosely as elements which occur significantly more frequently
 * in the set than the remainder of the elements. More precisely, Frequent Elements are generally split into two
 * categories: <em>Heavy Hitters</em>(or <em>Icebergs</em>) and <em>Top K Elements</em>.
 *
 * <h1>Heavy Hitters</h1>
 *
 * Fix {@code x} as an element in the multiset {@code X}. Then randomly select an element {@code y} from the multiset.
 * Define the probability that {@code y==x} as {@code p(x)}. Then a <em>Heavy Hitter</em> in {@code X} is an element
 * {@code h} in {@code X} whose probably {@code p(h) > s}, for some fixed {@code s} in {@code [0,1)}, which we call the
 * <em>support</em>. All the elements whose probability exceeds this support are referred to as the
 * <em>Heavy Hitters</em> (sometimes literature refers to them as the <em>Icerbergs</em> of {@code X}).
 *
 * Heavy Hitters are not guaranteed to exist. Since the support is defined as a constant value which is chosen by the
 * user, it is always possible that the user chooses a support which is so high that no elements exceed that element. If
 * the support were chosen to be close to 1, for example, then the data set would have to contain only 1 element in
 * order for anything to be returned.
 *
 * <h1>Top K Elements</h1>
 *
 * The <em>Top-K</em> elements in a data set are the {@code k} elements in {@code X} which have the highest occurrence.
 * As long as the data set is not empty, there is always at least one top-k element, but there may be fewer than {@code
 * k} top-k elements. For example, if {@code k} is chosen to be 100, but there are only 50 distinct items in the set,
 * then there are only 50 top-k elements.
 *
 * <h1>Limitations and Other Considerations</h1>
 *
 * <h3>Uniform DataSets</h3>
 *
 * Frequent Elements are generally useful with data sets which are highly non-uniform (thankfully, there are
 * many such data sets in the world). When the underlying distribution of the dataset is very uniform, then the
 * usefulness of collecting these is somewhat questionable.
 *
 * @author Scott Fines
 * Date: 2/20/15
 */
package com.splicemachine.stats.frequency;