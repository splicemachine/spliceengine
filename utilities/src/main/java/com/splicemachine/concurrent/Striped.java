/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.concurrent;

import static com.google.common.base.Objects.firstNonNull;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.math.IntMath;
import com.google.common.primitives.Ints;

import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A striped {@code Lock/Semaphore/ReadWriteLock}. This offers the underlying lock striping
 * similar to that of {@code ConcurrentHashMap} in a reusable form, and extends it for
 * semaphores and read-write locks. Conceptually, lock striping is the technique of dividing a lock
 * into many <i>stripes</i>, increasing the granularity of a single lock and allowing independent
 * operations to lock different stripes and proceed concurrently, instead of creating contention
 * for a single lock.
 *
 * <p>The guarantee provided by this class is that equal keys lead to the same lock (or semaphore),
 * i.e. {@code if (key1.equals(key2))} then {@code striped.get(key1) == striped.get(key2)}
 * (assuming {@link Object#hashCode()} is correctly implemented for the keys). Note
 * that if {@code key1} is <strong>not</strong> equal to {@code key2}, it is <strong>not</strong>
 * guaranteed that {@code striped.get(key1) != striped.get(key2)}; the elements might nevertheless
 * be mapped to the same lock. The lower the number of stripes, the higher the probability of this
 * happening.
 *
 * <p>There are three flavors of this class: {@code Striped<Lock>}, {@code Striped<Semaphore>},
 * and {@code Striped<ReadWriteLock>}. For each type, two implementations are offered:
 * {@linkplain #lock(int) strong} and {@linkplain #lazyWeakLock(int) weak}
 * {@code Striped<Lock>}, {@linkplain #semaphore(int, int) strong} and {@linkplain
 * #lazyWeakSemaphore(int, int) weak} {@code Striped<Semaphore>}, and {@linkplain
 * #readWriteLock(int) strong} and {@linkplain #lazyWeakReadWriteLock(int) weak}
 * {@code Striped<ReadWriteLock>}. <i>Strong</i> means that all stripes (locks/semaphores) are
 * initialized eagerly, and are not reclaimed unless {@code Striped} itself is reclaimable.
 * <i>Weak</i> means that locks/semaphores are created lazily, and they are allowed to be reclaimed
 * if nobody is holding on to them. This is useful, for example, if one wants to create a {@code
 * Striped<Lock>} of many locks, but worries that in most cases only a small portion of these
 * would be in use.
 *
 * <p>Prior to this class, one might be tempted to use {@code Map<K, Lock>}, where {@code K}
 * represents the task. This maximizes concurrency by having each unique key mapped to a unique
 * lock, but also maximizes memory footprint. On the other extreme, one could use a single lock
 * for all tasks, which minimizes memory footprint but also minimizes concurrency. Instead of
 * choosing either of these extremes, {@code Striped} allows the user to trade between required
 * concurrency and memory footprint. For example, if a set of tasks are CPU-bound, one could easily
 * create a very compact {@code Striped<Lock>} of {@code availableProcessors() * 4} stripes,
 * instead of possibly thousands of locks which could be created in a {@code Map<K, Lock>}
 * structure.
 *
 * -sf- Note, this class is stolen in its entirety from Google guava, version 15.0.0. It is reproduced here
 * to avoid incurring potential Guava classpath conflicts.
 *
 * @author Dimitris Andreou
 * @since 13.0
 */
@Beta
public abstract class Striped<L> {
    private Striped() {}

    /**
     * Returns the stripe that corresponds to the passed key. It is always guaranteed that if
     * {@code key1.equals(key2)}, then {@code get(key1) == get(key2)}.
     *
     * @param key an arbitrary, non-null key
     * @return the stripe that the passed key corresponds to
     */
    public abstract L get(Object key);

    /**
     * Returns the stripe at the specified index. Valid indexes are 0, inclusively, to
     * {@code size()}, exclusively.
     *
     * @param index the index of the stripe to return; must be in {@code [0...size())}
     * @return the stripe at the specified index
     */
    public abstract L getAt(int index);

    /**
     * Returns the index to which the given key is mapped, so that getAt(indexFor(key)) == get(key).
     */
    abstract int indexFor(Object key);

    /**
     * Returns the total number of stripes in this instance.
     */
    public abstract int size();

    /**
     * Returns the stripes that correspond to the passed objects, in ascending (as per
     * {@link #getAt(int)}) order. Thus, threads that use the stripes in the order returned
     * by this method are guaranteed to not deadlock each other.
     *
     * <p>It should be noted that using a {@code Striped<L>} with relatively few stripes, and
     * {@code bulkGet(keys)} with a relative large number of keys can cause an excessive number
     * of shared stripes (much like the birthday paradox, where much fewer than anticipated birthdays
     * are needed for a pair of them to match). Please consider carefully the implications of the
     * number of stripes, the intended concurrency level, and the typical number of keys used in a
     * {@code bulkGet(keys)} operation. See <a href="http://www.mathpages.com/home/kmath199.htm">Balls
     * in Bins model</a> for mathematical formulas that can be used to estimate the probability of
     * collisions.
     *
     * @param keys arbitrary non-null keys
     * @return the stripes corresponding to the objects (one per each object, derived by delegating
     *         to {@link #get(Object)}; may contain duplicates), in an increasing index order.
     */
    public Iterable<L> bulkGet(Iterable<?> keys) {
        // Initially using the array to store the keys, then reusing it to store the respective L's
        final Object[] array = Iterables.toArray(keys, Object.class);
        int[] stripes = new int[array.length];
        for (int i = 0; i < array.length; i++) {
            stripes[i] = indexFor(array[i]);
        }
        Arrays.sort(stripes);
        for (int i = 0; i < array.length; i++) {
            array[i] = getAt(stripes[i]);
        }
    /*
     * Note that the returned Iterable holds references to the returned stripes, to avoid
     * error-prone code like:
     *
     * Striped<Lock> stripedLock = Striped.lazyWeakXXX(...)'
     * Iterable<Lock> locks = stripedLock.bulkGet(keys);
     * for (Lock lock : locks) {
     *   lock.lock();
     * }
     * operation();
     * for (Lock lock : locks) {
     *   lock.unlock();
     * }
     *
     * If we only held the int[] stripes, translating it on the fly to L's, the original locks
     * might be garbage collected after locking them, ending up in a huge mess.
     */
        @SuppressWarnings("unchecked") // we carefully replaced all keys with their respective L's
                List<L> asList = (List<L>) Arrays.asList(array);
        return Collections.unmodifiableList(asList);
    }

    // Static factories

    /**
     * Creates a {@code Striped<Lock>} with eagerly initialized, strongly referenced locks.
     * Every lock is reentrant.
     *
     * @param stripes the minimum number of stripes (locks) required
     * @return a new {@code Striped<Lock>}
     */
    public static Striped<Lock> lock(int stripes) {
        return new CompactStriped<Lock>(stripes, new Supplier<Lock>() {
            public Lock get() {
                return new PaddedLock();
            }
        });
    }

    /**
     * Creates a {@code Striped<Lock>} with lazily initialized, weakly referenced locks.
     * Every lock is reentrant.
     *
     * @param stripes the minimum number of stripes (locks) required
     * @return a new {@code Striped<Lock>}
     */
    public static Striped<Lock> lazyWeakLock(int stripes) {
        return new LazyStriped<Lock>(stripes, new Supplier<Lock>() {
            public Lock get() {
                return new ReentrantLock(false);
            }
        });
    }

    /**
     * Creates a {@code Striped<Semaphore>} with eagerly initialized, strongly referenced semaphores,
     * with the specified number of permits.
     *
     * @param stripes the minimum number of stripes (semaphores) required
     * @param permits the number of permits in each semaphore
     * @return a new {@code Striped<Semaphore>}
     */
    public static Striped<Semaphore> semaphore(int stripes, final int permits) {
        return new CompactStriped<Semaphore>(stripes, new Supplier<Semaphore>() {
            public Semaphore get() {
                return new PaddedSemaphore(permits);
            }
        });
    }

    /**
     * Creates a {@code Striped<Semaphore>} with lazily initialized, weakly referenced semaphores,
     * with the specified number of permits.
     *
     * @param stripes the minimum number of stripes (semaphores) required
     * @param permits the number of permits in each semaphore
     * @return a new {@code Striped<Semaphore>}
     */
    public static Striped<Semaphore> lazyWeakSemaphore(int stripes, final int permits) {
        return new LazyStriped<Semaphore>(stripes, new Supplier<Semaphore>() {
            public Semaphore get() {
                return new Semaphore(permits, false);
            }
        });
    }

    /**
     * Creates a {@code Striped<ReadWriteLock>} with eagerly initialized, strongly referenced
     * read-write locks. Every lock is reentrant.
     *
     * @param stripes the minimum number of stripes (locks) required
     * @return a new {@code Striped<ReadWriteLock>}
     */
    public static Striped<ReadWriteLock> readWriteLock(int stripes) {
        return new CompactStriped<ReadWriteLock>(stripes, READ_WRITE_LOCK_SUPPLIER);
    }

    /**
     * Creates a {@code Striped<ReadWriteLock>} with lazily initialized, weakly referenced
     * read-write locks. Every lock is reentrant.
     *
     * @param stripes the minimum number of stripes (locks) required
     * @return a new {@code Striped<ReadWriteLock>}
     */
    public static Striped<ReadWriteLock> lazyWeakReadWriteLock(int stripes) {
        return new LazyStriped<ReadWriteLock>(stripes, READ_WRITE_LOCK_SUPPLIER);
    }

    // ReentrantReadWriteLock is large enough to make padding probably unnecessary
    private static final Supplier<ReadWriteLock> READ_WRITE_LOCK_SUPPLIER =
            new Supplier<ReadWriteLock>() {
                public ReadWriteLock get() {
                    return new ReentrantReadWriteLock();
                }
            };

    private abstract static class PowerOfTwoStriped<L> extends Striped<L> {
        /** Capacity (power of two) minus one, for fast mod evaluation */
        final int mask;

        PowerOfTwoStriped(int stripes) {
            Preconditions.checkArgument(stripes > 0, "Stripes must be positive");
            this.mask = stripes > Ints.MAX_POWER_OF_TWO ? ALL_SET : ceilToPowerOfTwo(stripes) - 1;
        }

        @Override final int indexFor(Object key) {
            int hash = smear(key.hashCode());
            return hash & mask;
        }

        @Override public final L get(Object key) {
            return getAt(indexFor(key));
        }
    }

    /**
     * Implementation of Striped where 2^k stripes are represented as an array of the same length,
     * eagerly initialized.
     */
    private static class CompactStriped<L> extends PowerOfTwoStriped<L> {
        /** Size is a power of two. */
        private final Object[] array;

        private CompactStriped(int stripes, Supplier<L> supplier) {
            super(stripes);
            Preconditions.checkArgument(stripes <= Ints.MAX_POWER_OF_TWO, "Stripes must be <= 2^30)");

            this.array = new Object[mask + 1];
            for (int i = 0; i < array.length; i++) {
                array[i] = supplier.get();
            }
        }

        @SuppressWarnings("unchecked") // we only put L's in the array
        @Override public L getAt(int index) {
            return (L) array[index];
        }

        @Override public int size() {
            return array.length;
        }
    }

    /**
     * Implementation of Striped where up to 2^k stripes can be represented, using a Cache
     * where the key domain is [0..2^k). To map a user key into a stripe, we take a k-bit slice of the
     * user key's (smeared) hashCode(). The stripes are lazily initialized and are weakly referenced.
     */
    private static class LazyStriped<L> extends PowerOfTwoStriped<L> {
        final ConcurrentMap<Integer, L> locks;
        final Supplier<L> supplier;
        final int size;

        LazyStriped(int stripes, Supplier<L> supplier) {
            super(stripes);
            this.size = (mask == ALL_SET) ? Integer.MAX_VALUE : mask + 1;
            this.supplier = supplier;
            this.locks = new MapMaker().weakValues().makeMap();
        }

        @Override public L getAt(int index) {
            if (size != Integer.MAX_VALUE) {
                Preconditions.checkElementIndex(index, size());
            } // else no check necessary, all index values are valid
            L existing = locks.get(index);
            if (existing != null) {
                return existing;
            }
            L created = supplier.get();
            existing = locks.putIfAbsent(index, created);
            return firstNonNull(existing, created);
        }

        @Override public int size() {
            return size;
        }
    }

    /**
     * A bit mask were all bits are set.
     */
    private static final int ALL_SET = ~0;

    private static int ceilToPowerOfTwo(int x) {
        return 1 << IntMath.log2(x, RoundingMode.CEILING);
    }

    /*
     * This method was written by Doug Lea with assistance from members of JCP
     * JSR-166 Expert Group and released to the public domain, as explained at
     * http://creativecommons.org/licenses/publicdomain
     *
     * As of 2010/06/11, this method is identical to the (package private) hash
     * method in OpenJDK 7's java.util.HashMap class.
     */
    // Copied from java/com/google/common/collect/Hashing.java
    private static int smear(int hashCode) {
        hashCode ^= (hashCode >>> 20) ^ (hashCode >>> 12);
        return hashCode ^ (hashCode >>> 7) ^ (hashCode >>> 4);
    }

    private static class PaddedLock extends ReentrantLock {
        /*
         * Padding from 40 into 64 bytes, same size as cache line. Might be beneficial to add
         * a fourth long here, to minimize chance of interference between consecutive locks,
         * but I couldn't observe any benefit from that.
         */
        @SuppressWarnings("unused")
        long q1, q2, q3;

        PaddedLock() {
            super(false);
        }
    }

    private static class PaddedSemaphore extends Semaphore {
        // See PaddedReentrantLock comment
        @SuppressWarnings("unused")
        long q1, q2, q3;

        PaddedSemaphore(int permits) {
            super(permits, false);
        }
    }
}
