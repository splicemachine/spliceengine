package com.splicemachine.derby.impl.job.scheduler;

import java.util.concurrent.RunnableFuture;

/**
 * @author Scott Fines
 *         Created on: 4/12/13
 */
public interface PriorityRunnable<T> extends RunnableFuture<T> {

    int getPriority();
}
