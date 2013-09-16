package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.base.Function;
import com.splicemachine.si.impl.TransactionId;

import java.util.concurrent.ExecutionException;

public class SchedulerTracer {
    private static transient Runnable fTaskStart = null;
    private static transient Runnable fTaskEnd = null;
    private static transient Function<TransactionId, Object> fTaskCommit = null;

    public static void registerTaskStart(Runnable f) {
        fTaskStart = f;
    }

    public static void registerTaskEnd(Runnable f) {
        fTaskEnd = f;
    }

    public static void registerTaskCommit(Function<TransactionId, Object> f) {
        fTaskCommit = f;
    }

    public static void traceTaskStart() throws ExecutionException {
        if (fTaskStart != null) {
            try {
                fTaskStart.run();
            } catch (RuntimeException e) {
                throw (ExecutionException) e.getCause();
            }
        }
    }

    public static void traceTaskEnd() {
        if (fTaskEnd != null) {
            fTaskEnd.run();
        }
    }

    public static void traceTaskCommit(TransactionId transactionId) {
        if (fTaskCommit != null) {
            fTaskCommit.apply(transactionId);
        }
    }
}
