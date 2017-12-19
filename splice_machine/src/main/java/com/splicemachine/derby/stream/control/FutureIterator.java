package com.splicemachine.derby.stream.control;

import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.Future;

import static java.util.Arrays.asList;

/**
 *
 * Iterator over Futures
 *
 *
 */
public class FutureIterator<T> implements Iterator<T> {

    private List<Future<Iterator<T>>> futureIterators;
    private Iterator<T> current;


    public FutureIterator() {
        this.futureIterators = new LinkedList<>();
    }

    @SafeVarargs
    public FutureIterator(Future<Iterator<T>>... futureIterators) {
        this.futureIterators = new LinkedList<>(asList(futureIterators));
    }

    public void appendFutureIterator(Future<Iterator<T>> iteratorFuture) {
        this.futureIterators.add(iteratorFuture);
    }

    @Override
    public boolean hasNext() {
        try {
            while (true) {
                if (current == null) {
                    if (futureIterators.isEmpty())
                        return false;
                    else
                        current = futureIterators.remove(0).get();
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




