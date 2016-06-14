package com.splicemachine.stream;

/**
 * @author Scott Fines
 *         Date: 2/3/15
 */
public interface Predicate<T> {

    public boolean apply(T element) throws StreamException;
}
