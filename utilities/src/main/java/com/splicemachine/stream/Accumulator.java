package com.splicemachine.stream;

/**
 * @author Scott Fines
 * Date: 8/13/14
 */
public interface Accumulator<T> {

    void accumulate(T next) throws StreamException;


}
