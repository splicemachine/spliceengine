package com.splicemachine.job;

import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public interface TaskScheduler<T extends Task> {

    TaskFuture submit(T task) throws ExecutionException;


}
