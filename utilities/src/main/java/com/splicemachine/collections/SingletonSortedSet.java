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

package com.splicemachine.collections;

import com.splicemachine.utils.ComparableComparator;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 4/15/14
 */
public class SingletonSortedSet<E>  extends AbstractSet<E> implements SortedSet<E> {
		private E value;
    private Comparator<? super E> comparator;

		public SingletonSortedSet(E value,Comparator<? super E> comparator) {
        this.value = value;
        this.comparator = comparator;
    }

    public static <E extends Comparable<E>> SortedSet<E> wrap(E value){
        return new SingletonSortedSet<>(value,ComparableComparator.<E>newComparator());
    }

		@Override public Iterator<E> iterator() {
        return new SingletonIterator<E>(value);
    }

		@Override public int size() { return 1; }
		@Override public Comparator<? super E> comparator() { return comparator;}

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
