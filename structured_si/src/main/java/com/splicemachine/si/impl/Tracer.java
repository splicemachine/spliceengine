package com.splicemachine.si.impl;

import com.google.common.base.Function;

public class Tracer {
    private static transient Function<Object, Object> f = null;
    private static transient Function<Long, Object> fTransaction = null;
    public static Integer rollForwardDelayOverride = null;

    public static void register(Function<Object, Object> f) {
        Tracer.f = f;
    }

    public static void registerTransaction(Function<Long, Object> f) {
        Tracer.fTransaction = f;
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
}
