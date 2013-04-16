package com.splicemachine.job;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public interface Task {

    void markStarted() throws ExecutionException, CancellationException;

    void markCompleted() throws ExecutionException;

    void markFailed(Throwable error) throws ExecutionException;

    void markCancelled() throws ExecutionException;

    void execute() throws ExecutionException,InterruptedException;

    boolean isCancelled() throws ExecutionException;

    String getTaskId();

    TaskStatus getTaskStatus();

    void markInvalid() throws ExecutionException;

    boolean isInvalidated();

    void cleanup() throws ExecutionException;

    int getPriority();
}
