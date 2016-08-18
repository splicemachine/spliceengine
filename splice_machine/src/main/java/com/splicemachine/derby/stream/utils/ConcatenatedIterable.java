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
 *
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

