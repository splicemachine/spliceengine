package com.splicemachine.job;

import com.splicemachine.derby.stats.TaskStats;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public interface TaskFuture {

    public Status getStatus() throws ExecutionException;

    public void complete() throws ExecutionException,CancellationException,InterruptedException;

    public double getEstimatedCost();

//    void cancel() throws ExecutionException;

    String getTaskId();

    TaskStats getTaskStats();
}
