package com.splicemachine.si.impl;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * Provides hooks for tests to provide callbacks. Mainly used to provide thread coordination in tests. It allows tests
 * to "trace" the internals of the SI execution.
 */
public class Tracer {
    private static transient Function<Object, Object> fRowRollForward = null;
    private static transient Function<Long, Object> fTransactionRollForward = null;
    private static transient Function<Object[], Object> fStatus = null;
    public static transient Runnable fCompact = null;
    private static transient Function<Long, Object> fCommitting = null;
    private static transient Function<Long, Object> fWaiting = null;
    private static transient Function<Object[], Object> fRegion = null;

    public static Integer rollForwardDelayOverride = null;

    public static void registerRowRollForward(Function<Object, Object> f) {
        Tracer.fRowRollForward = f;
    }

    public static void registerTransactionRollForward(Function<Long, Object> f) {
        Tracer.fTransactionRollForward = f;
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

    public static void registerRegion(Function<Object[], Object> f) {
        Tracer.fRegion = f;
    }

    public static void traceRowRollForward(Object key) {
        if (fRowRollForward != null) {
            fRowRollForward.apply(key);
        }
    }

    public static void traceTransactionRollForward(long transactionId) {
        if (fTransactionRollForward != null) {
            fTransactionRollForward.apply(transactionId);
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

    public static void traceRegion(String tableName, HRegion region) {
        if (fRegion != null) {
            fRegion.apply(new Object[] {tableName, region});
        }
    }

}