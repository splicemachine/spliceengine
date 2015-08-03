package com.splicemachine.concurrent;

/**
 * @author Scott Fines
 *         Date: 8/3/15
 */
public class SystemClock implements Clock{
    public static final Clock INSTANCE = new SystemClock();
    @Override public long currentTimeMillis(){ return System.currentTimeMillis(); }
    @Override public long nanoTime(){ return System.nanoTime(); }
}
