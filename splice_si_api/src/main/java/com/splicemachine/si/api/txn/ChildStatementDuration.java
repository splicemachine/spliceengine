package com.splicemachine.si.api.txn;

import java.io.Externalizable;

/**
 *
 *
 */
public interface ChildStatementDuration extends Externalizable {
    int getChildTransactionId();
    String getChildStatementId();
    long getDuration();

}
