package com.splicemachine.utils;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 4/15/14
 */
public class SingletonSortedSet<E extends Comparable<E>> extends AbstractSet<E> implements SortedSet<E> {
		private E value;

		public SingletonSortedSet(E value) { this.value = value; }

		@Override public Iterator<E> iterator() {
        return new SingletonIterator<E>(value);
    }

		@Override public int size() { return 1; }
		@Override public Comparator<? super E> comparator() { return ComparableComparator.<E>newComparator(); }

		@Override public SortedSet<E> subSet(E fromElement, E toElement) { return this; }
		@Override public SortedSet<E> headSet(E toElement) { return this; }
		@Override public SortedSet<E> tailSet(E fromElement) { return this; }
		@Override public E first() { return value; }
		@Override public E last() { return value; }

    private static class SingletonIterator<E> implements Iterator<E>{
        private E item;
        private boolean done = false;

        private SingletonIterator(E item) {
            this.item = item;
        }

        @Override
        public boolean hasNext() {
            return !done;
        }

        @Override
        public E next() {
            if(done) throw new NoSuchElementException();
            done = true;
            return item;
        }

        @Override public void remove() { throw new UnsupportedOperationException(); }
    }


}
