package com.splicemachine.concurrent;

/**
 * @author Scott Fines
 *         Date: 8/3/15
 */
public interface Clock{

    long currentTimeMillis();

    long nanoTime();
}
