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
 * Fix {@code x} as an element in the multiset {@code X}, and {@code s} in {@code [0,1)}.
 * Then randomly select an element {@code y} from the multiset, and define the probability that
 * {@code y==x} as {@code p(x)}. Then a <em>Heavy Hitter</em> in {@code X} is an element
 * {@code h} in {@code X} whose probability {@code p(h) > s}. In this case, we call {@code s} the
 * <em>support</em>. All the elements whose probability exceeds the support are referred to as the
 * <em>Heavy Hitters</em> (sometimes literature refers to them as the <em>Icerbergs</em> of {@code X}).
 *
 * Heavy Hitters are not guaranteed to exist. Since the support is defined as a constant value which is chosen by the
 * user, it is always possible that the user chooses a support which is so high that no elements exceed that element. If
 * the support were chosen to be close to 1, for example, then the data set would have to contain only 1 element in
 * order for anything to be returned.
 *
 * <h2>Algorithms</h2>
 * There are multiple algorithms which are known to compute the Heavy Hitters in a data set. However, any
 * algorithm which exactly computes the Heavy Hitters requires {@code \u03A9(N)} space to compute, which is generally
 * more than we are willing to spend in most applications. It thus behooves us to find approximate algorithms, which
 * are not as correct, but which use less space.
 *
 * The default implementations contained within this package use the <em>SpaceSaver</em> algorithm (which
 * can also be used to compute the Top K elements), devised by Metwally et. al in
 * "<em>Efficient Computation of Frequent and Top-k elements in Data Streams</em>". This algorithm uses a constant,
 * configurable amount of memory to compute the Heavy Hitters approximately.
 *
 * Additional algorithms include the <em>Lossy Counting</em> family (<em>Probabilistic Lossy Counting</em>, devised
 *  by Dimitropoulous et. al. in "<em>Probabilistic Lossy Counting: An efficient algorithm for finding heavy hitters</em>"
 *  is the most recent version of this family) and <em>Sticky Sampling</em>(devised by Manku and Motwani in
 *  "<em>Approximate Frequency Counts over Data Streams</em>"). These algorithms are available, but are not
 *  implemented (as of Feb. 2015) in this package directly.
 *
 * <h1>Top K Elements</h1>
 *
 * The <em>Top-K</em> elements in a data set are the {@code k} elements in {@code X} which have the highest occurrence.
 * As long as the data set is not empty, there is always at least one top-k element, but there may be fewer than {@code
 * k} top-k elements. For example, if {@code k} is chosen to be 100, but there are only 50 distinct items in the set,
 * then there are only 50 top-k elements.
 *
 * <h2>Algorithms</h2>
 * It is fairly clear upon reflection that the Top-K problem and the Heavy Hitters problem are highly related. For
 * example, finding the top 100 more frequent items is guaranteed to also find all elements which occur in more than 1%
 * of the data set. Similarly, finding all elements which occur more frequently than {@code s} times in the data
 * set is guaranteed to find the {@code s} most frequent elements. Thus, any algorithm which can be used
 * for the finding of Heavy Hitters can also be used to find the Top-K elements.
 *
 * In this package, we implement the <em>SpaceSaver</em> algorithm for both heavy hitters and Top-K results.
 *
 * <h1>Limitations and Other Considerations</h1>
 *
 * <h3>Uniform DataSets</h3>
 *
 * Frequent Elements are generally useful with data sets which are highly non-uniform (thankfully, there are
 * many such data sets in the world). When the underlying distribution of the dataset is very uniform, then the
 * usefulness of collecting these is somewhat questionable.
 *
 * <h1>API structure</h1>
 *
 * Classes are generally package-private and represented by a public interface (where meaningful). There are
 * two main interfaces: {@link com.splicemachine.stats.frequency.FrequencyCounter}
 * and {@link com.splicemachine.stats.frequency.FrequentElements}. The {@code FrequencyCounter} interface represents
 * an updateable interface which is intended to be used over a stream of values. At any time during (or after)
 * this stream, a request can be made to collect either the Top-K elements or HeavyHitters into a
 * {@code FrequentElements}, which can be used to answer numerous queries based on ranges of comparable values
 * where the type is Comparable, and on equality queries when the type is not.
 *
 * {@code FrequentElement} instances can be merged together, as they represent linear transformations of the
 * underlying data set. This allows for trivial parallelization and distribution of workload when necessary,
 * although the FrequencyCounter and FrequentElements classes are not generally internally thread-safe.
 *
 * There are multiple subinterfaces and subclasses of the {@code FrequentElements} and {@code FrequencyCounter}
 * interfaces, which specialize the interface for particular primitive types. These primarily exist in order
 * to avoid autoboxing concerns, but may be used to provide specialized implementations for specific data types (such
 * as booleans or single byte versions, which are simple to implement, or byte arrays, which have complex
 * access patterns).
 *
 * A new {@code FrequencyCounter} can be constructed statically using methods provided in the
 * {@link com.splicemachine.stats.frequency.FrequencyCounters} class. These methods provide reasonable defaults,
 * as well as options to override those defaults where useful.
 *
 * @author Scott Fines
 *         Date: 2/20/15
 */
package com.splicemachine.stats.frequency;