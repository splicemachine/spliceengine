package com.splicemachine.si.impl;

import com.google.common.base.Function;

/**
 * Provides hooks for tests to provide callbacks. Mainly used to provide thread coordination in tests. It allows tests
 * to "trace" the internals of the SI execution.
 */
public class Tracer {
    private static transient Function<Object, Object> f = null;
    private static transient Function<Long, Object> fTransaction = null;
    private static transient Function<Object[], Object> fStatus = null;
    public static transient Runnable fCompact = null;
    private static transient Function<Long, Object> fCommitting = null;
    private static transient Function<Long, Object> fWaiting = null;

    public static Integer rollForwardDelayOverride = null;

    public static void register(Function<Object, Object> f) {
        Tracer.f = f;
    }

    public static void registerTransaction(Function<Long, Object> f) {
        Tracer.fTransaction = f;
    }

    public static void registerStatus(Function<Object[], Object> f) {
        Tracer.fStatus = f;
    }

    public static void registerCompact(Runnable f) {
        Tracer.fCompact = f;
    }

    public static void registerCommitting(Function<Long, Object> f) {
        Tracer.fCommitting = f;
    }

    public static void registerWaiting(Function<Long, Object> f) {
        Tracer.fWaiting = f;
    }

    public static void trace(Object key) {
        if (f != null) {
            f.apply(key);
        }
    }

    public static void traceTransaction(long transactionId) {
        if (fTransaction != null) {
            fTransaction.apply(transactionId);
        }
    }

    public static void traceStatus(long transactionId, TransactionStatus newStatus, boolean beforeChange) {
        if (fStatus != null) {
            fStatus.apply(new Object[] {transactionId, newStatus, beforeChange});
        }
    }

    public static void compact() {
        if (fCompact != null) {
            fCompact.run();
        }
    }

    public static void traceCommitting(long transactionId) {
        if (fCommitting != null) {
            fCommitting.apply(transactionId);
        }
    }

    public static void traceWaiting(long transactionId) {
        if (fWaiting != null) {
            fWaiting.apply(transactionId);
        }
    }

}