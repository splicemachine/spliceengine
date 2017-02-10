/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.impl;

import org.spark_project.guava.base.Function;
import com.splicemachine.si.api.txn.TransactionStatus;

/**
 * Provides hooks for tests to provide callbacks. Mainly used to provide thread coordination in tests. It allows tests
 * to "trace" the internals of the SI execution.
 */
public class Tracer {
    private static transient Function<byte[],byte[]> fRowRollForward = null;
    private static transient Function<Long, Object> fTransactionRollForward = null;
    private static transient Function<Object[], Object> fStatus = null;
    private static transient Runnable fCompact = null;
    private static transient Function<Long, Object> fCommitting = null;
    private static transient Function<Long, Object> fWaiting = null;
    private static transient Function<Object[], Object> fRegion = null;
    private static transient Function<Object, String> bestAccess = null; 

    public static Integer rollForwardDelayOverride = null;

    public static void registerRowRollForward(Function<byte[],byte[]> f) {
        Tracer.fRowRollForward = f;
    }
    
    public static boolean isTracingRowRollForward() {
    	return Tracer.fRowRollForward != null;
    }

    public static void registerTransactionRollForward(Function<Long, Object> f) {
        Tracer.fTransactionRollForward = f;
    }

    public static boolean isTracingTransactionRollForward() {
    	return Tracer.fTransactionRollForward != null;
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

    public static void registerBestAccess(Function<Object, String> f) {
        Tracer.bestAccess = f;
    }

    public static void registerWaiting(Function<Long, Object> f) {
        Tracer.fWaiting = f;
    }

    public static void registerRegion(Function<Object[], Object> f) {
        Tracer.fRegion = f;
    }

    public static void traceRowRollForward(byte[] key) {
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

    public static void traceRegion(String tableName, Object region) {
        if (fRegion != null) {
            fRegion.apply(new Object[] {tableName, region});
        }
    }

    public static void traceBestAccess(Object objectParam) {
        if (bestAccess != null) {
        	bestAccess.apply(objectParam);
        }
    }

}