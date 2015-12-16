package com.splicemachine.concurrent;

import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 8/3/15
 */
public interface Clock{

    long currentTimeMillis();

    long nanoTime();

    void sleep(long time,TimeUnit unit) throws InterruptedException;
}
