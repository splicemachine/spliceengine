package com.splicemachine.si.api.txn;

/**
 *
 *
 */
public interface ChildStatementDuration {
    int getChildTransactionId();
    String getChildStatementId();
    long getDuration();

}
