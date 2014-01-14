package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.base.Function;
import com.splicemachine.si.impl.TransactionId;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class SchedulerTracer {
    private static volatile Callable<Void> fTaskStart = null;
    private static volatile Callable<Void> fTaskEnd = null;
    private static volatile Function<TransactionId, Object> fTaskCommit = null;
    private static volatile Function<TransactionId,Object> fTaskRollback = null;

    public static void registerTaskStart(Callable<Void> f) {
        fTaskStart = f;
    }

    public static void registerTaskEnd(Callable<Void> f) {
        fTaskEnd = f;
    }

    public static void registerTaskCommit(Function<TransactionId, Object> f) {
        fTaskCommit = f;
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

    public static void traceTaskCommit(TransactionId transactionId) {
        if (fTaskCommit != null) {
            fTaskCommit.apply(transactionId);
        }
    }

    public static void traceTaskRollback(TransactionId txnId) {
        if(fTaskRollback !=null)
            fTaskRollback.apply(txnId);
    }

    public static void registerTaskRollback(Function<TransactionId, Object> taskRollback) {
        fTaskRollback = taskRollback;
    }
}
