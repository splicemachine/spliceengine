package com.splicemachine.si2.si.api;

import org.apache.log4j.Logger;

public class ParentTransactionManager {
    static final Logger LOG = Logger.getLogger(ParentTransactionManager.class);
    private static ThreadLocal<String> parentTransactionIdThreadLocal = new ThreadLocal<String>();

    public static void setParentTransactionId(String transactionId) {
        parentTransactionIdThreadLocal.set(transactionId);
    }

    public static String getParentTransactionId() {
        return parentTransactionIdThreadLocal.get();
    }
}