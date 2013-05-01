package com.splicemachine.si.impl;

import com.google.common.base.Function;

public class Tracer {
    private static transient Function<Object, Object> f = null;
    private static transient Function<Long, Object> fTransaction = null;
    private static transient Function<Object[], Object> fStatus = null;
    public static transient Runnable fCompact = null;

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

}