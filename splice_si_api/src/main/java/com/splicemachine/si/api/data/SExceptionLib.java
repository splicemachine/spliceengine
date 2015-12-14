package com.splicemachine.si.api.data;

import com.splicemachine.si.api.txn.Txn;

import java.io.IOException;

/**
 * Created by jleach on 12/11/15.
 */
public interface SExceptionLib {
    IOException getWriteConflictException(long txn1,long txn2);
    IOException getReadOnlyModificationException(String message);
    IOException getNoSuchColumnFamilyException(String message);
    IOException getTransactionTimeoutException(long tnxId);
    IOException getCannotCommitException(long txnId,Txn.State actualState);
    IOException getAdditiveWriteConflict();
    IOException getDoNotRetryIOException(String message);
}