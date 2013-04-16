package com.splicemachine.derby.impl.job.scheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * @author Scott Fines
 *         Created on: 4/12/13
 */
public class CallableAccessibleFutureTask<V> extends FutureTask<V> {
    private final Callable<V> callable;
    public CallableAccessibleFutureTask(Callable<V> callable) {
        super(callable);
        this.callable = callable;
    }

    public Callable<V> getCallable(){
        return callable;
    }

}
