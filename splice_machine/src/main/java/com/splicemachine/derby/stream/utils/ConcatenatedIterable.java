/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.utils;


import java.util.*;

/**
 * @author Scott Fines
 *         Date: 8/18/16
 */
public class ConcatenatedIterable<E> implements Iterable<E>{
    /*
     * This represents a concatenation of multiple iterables together without needing a lot of
     * stack allocations (see SPLICE-836).
     *
     * The initial implementation was to make use of guava's Iterable.concat() implementation. However,
     * each time you use Iterables.concat(), you create a new wrapper iterable around what we are concatenating,
     * which means that, for each row on the left, we would be creating a new Iterable wrapper around the
     * previous row's results. This is a fine recursive algorithm when we have tail-call optimizations, but
     * alas Java is not Scala, and tail-call optimizations don't exist, so we end up overflowing the stack
     * whenever the size of the left hand side is large enough.
     *
     * This implementation avoids the extra wrapper objects, resulting in a flat iterative algorithm for
     * concatenating all the values together. This should not only avoid StackOverflow problems, but in most cases
     * it should perform significantly better as well(since it will avoid creating millions of extraneous
     * Iterable objects).
     */
    private final List<Iterable<E>> iterables;

    public ConcatenatedIterable(List<Iterable<E>> iterables){
        this.iterables=iterables;
    }

    @Override
    public Iterator<E> iterator(){
        Queue<Iterator<E>> iterators=new LinkedList<>();
        for(Iterable<E> iterable : iterables){
            iterators.add(iterable.iterator());
        }

        return new ConcatenatedIterator<>(iterators);
    }

    private static class ConcatenatedIterator<E> implements Iterator<E>{
        private final Queue<Iterator<E>> iterators;
        private Iterator<E> currentIterator;

        ConcatenatedIterator(Queue<Iterator<E>> iterators){
            this.iterators=iterators;
        }

        @Override
        public boolean hasNext(){
            while(currentIterator==null || !currentIterator.hasNext()){
                currentIterator=iterators.poll();
                if(currentIterator==null) return false;
            }
            return true;
        }

        @Override
        public E next(){
            if(!hasNext()) throw new NoSuchElementException();
            return currentIterator.next();
        }

        @Override
        public void remove(){
            throw new UnsupportedOperationException("Removes not supported");
        }
    }
}

