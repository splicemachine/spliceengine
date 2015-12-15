package com.splicemachine.si.api.data;

import com.splicemachine.si.api.txn.Txn;

import java.io.IOException;

/**
 *
 * Created by jleach on 12/11/15.
 */
public interface ExceptionFactory{
    IOException writeWriteConflict(long txn1,long txn2);
    IOException readOnlyModification(String message);
    IOException noSuchFamily(String message);
    IOException transactionTimeout(long tnxId);
    IOException cannotCommit(long txnId,Txn.State actualState);
    IOException additiveWriteConflict();
    IOException doNotRetry(String message);
}