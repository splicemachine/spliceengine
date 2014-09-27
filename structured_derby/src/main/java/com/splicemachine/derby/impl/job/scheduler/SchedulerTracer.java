package com.splicemachine.derby.impl.job.scheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class SchedulerTracer {
    private static volatile Callable<Void> fTaskStart = null;
    private static volatile Callable<Void> fTaskEnd = null;

    public static void registerTaskStart(Callable<Void> f) {
        fTaskStart = f;
    }

    public static void registerTaskEnd(Callable<Void> f) {
        fTaskEnd = f;
    }

    public static void traceTaskStart() throws ExecutionException {
        if (fTaskStart != null) {
            try {
                fTaskStart.call();
            } catch (Exception e) {
                if(e instanceof ExecutionException)
                    throw (ExecutionException)e;
                throw new ExecutionException(e);
            }
        }
    }

    public static void traceTaskEnd() throws ExecutionException {
        if (fTaskEnd != null) {
            try {
                fTaskEnd.call();
            } catch (Exception e) {
                if(e instanceof ExecutionException)
                    throw (ExecutionException)e;
                throw new ExecutionException(e);
            }
        }
    }

}
