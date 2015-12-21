package com.splicemachine.si.impl.data;

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.MCannotCommitException;
import com.splicemachine.si.impl.MTransactionTimeout;
import com.splicemachine.si.impl.MWriteConflict;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MExceptionFactory implements ExceptionFactory{
    public static final ExceptionFactory INSTANCE= new MExceptionFactory();

    private MExceptionFactory(){}

    @Override
    public IOException writeWriteConflict(long txn1,long txn2){
        return new MWriteConflict(txn1,txn2);
    }

    @Override
    public IOException readOnlyModification(String message){
        return new MReadOnly(message);
    }

    @Override
    public IOException noSuchFamily(String message){
        return new MNoSuchFamily(message);
    }

    @Override
    public IOException transactionTimeout(long tnxId){
        return new MTransactionTimeout(tnxId);
    }

    @Override
    public IOException cannotCommit(long txnId,Txn.State actualState){
        return new MCannotCommitException(txnId,actualState);
    }

    @Override
    public IOException additiveWriteConflict(){
        return new MAdditiveWriteConflict();
    }

    @Override
    public IOException doNotRetry(String message){
        return new IOException(message);
    }
}
