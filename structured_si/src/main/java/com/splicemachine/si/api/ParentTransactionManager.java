package com.splicemachine.si.api;

import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

/**
 * Wrap a body of code in the calls necessary to setup and teardown a transaction context around it.
 */
public class ParentTransactionManager {
    static final Logger LOG = Logger.getLogger(ParentTransactionManager.class);
    private static ThreadLocal<String> parentTransactionIdThreadLocal = new ThreadLocal<String>();

    private static void setParentTransactionId(String transactionId) {
        parentTransactionIdThreadLocal.set(transactionId);
    }

    public static String getParentTransactionId() {
        return parentTransactionIdThreadLocal.get();
    }

    public static <T> T runInParentTransaction(String parentTransactionId, Callable<T> callable) throws Exception {
        final String oldParentTransactionId = ParentTransactionManager.getParentTransactionId();
        try {
            ParentTransactionManager.setParentTransactionId(parentTransactionId);
            return callable.call();
        } finally {
            ParentTransactionManager.setParentTransactionId(oldParentTransactionId);
        }
    }
}