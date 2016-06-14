package com.splicemachine.stream;

/**
 * @author Scott Fines
 *         Date: 8/13/14
 */
public interface Transformer<T,R> {

    public R transform(T element) throws StreamException;
}
