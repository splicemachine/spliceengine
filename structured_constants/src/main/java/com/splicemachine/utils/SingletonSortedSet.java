package com.splicemachine.utils;

import com.google.common.collect.Iterators;
import org.apache.commons.collections.comparators.ComparableComparator;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 4/15/14
 */
public class SingletonSortedSet<E extends Comparable<E>> extends AbstractSet<E> implements SortedSet<E> {
		private E value;

		public SingletonSortedSet(E value) { this.value = value; }

		@Override public Iterator<E> iterator() { return Iterators.singletonIterator(value); }

		@Override public int size() { return 1; }

		@Override public Comparator<? super E> comparator() { return ComparableComparator.getInstance(); }

		@Override
		public SortedSet<E> subSet(E fromElement, E toElement) {
				return this;
		}

		@Override
		public SortedSet<E> headSet(E toElement) {
				return this;
		}

		@Override
		public SortedSet<E> tailSet(E fromElement) {
				return this;
		}

		@Override
		public E first() {
				return value;
		}

		@Override
		public E last() {
				return value;
		}
}
