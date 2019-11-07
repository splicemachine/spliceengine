/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.control;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;

/**
 *
 * Iterator over Futures
 *
 *
 */
public class FutureIterator<T> implements Iterator<T> {

    private BlockingQueue<Future<Iterator<T>>> futureIterators;
    private volatile Iterator<T> current;


    public FutureIterator(int size) {
        this.futureIterators = new ArrayBlockingQueue<>(size);
    }

    @SafeVarargs
    public FutureIterator(Future<Iterator<T>>... futureIterators) {
        this.futureIterators = new ArrayBlockingQueue<>(futureIterators.length);
        this.futureIterators.addAll(asList(futureIterators));
    }

    public void appendFutureIterator(Future<Iterator<T>> iteratorFuture) {
        this.futureIterators.add(iteratorFuture);
    }

    @Override
    public boolean hasNext() {
        try {
            while (true) {
                if (current == null) {
                    Future<Iterator<T>> future = futureIterators.poll();
                    if (future == null)
                        return false;
                    else
                        current = future.get();
                } else {
                    if (current.hasNext())
                        return true;
                    else
                        current = null; // cycle
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T next() {
        return current.next();
    }

    @Override
    public void remove() {
        if (current == null) throw new IllegalStateException();
        current.remove();
    }

    @SafeVarargs
    public static <T> Iterator<T> concat(Future<Iterator<T>>... futureIterators) {
        return new FutureIterator<>(futureIterators);
    }

}




