package com.splicemachine.si.data;

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.*;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class HExceptionFactory implements ExceptionFactory{
    public static final HExceptionFactory INSTANCE = new HExceptionFactory();

    protected HExceptionFactory(){}
    @Override
    public IOException writeWriteConflict(long txn1,long txn2){
        return new HWriteConflict(txn1,txn2);
    }

    @Override
    public IOException readOnlyModification(String message){
        return new HReadOnlyModificationException(message);
    }

    @Override
    public IOException noSuchFamily(String message){
        return new NoSuchColumnFamilyException(message);
    }

    @Override
    public IOException transactionTimeout(long tnxId){
        return new HTransactionTimeout(tnxId);
    }

    @Override
    public IOException cannotCommit(long txnId,Txn.State actualState){
        return new HCannotCommitException(txnId,actualState);
    }

    @Override
    public IOException additiveWriteConflict(){
        return new AdditiveWriteConflict();
    }

    @Override
    public IOException doNotRetry(String message){
        return new DoNotRetryIOException(message);
    }

    @Override
    public IOException processRemoteException(Throwable e){
        if(e instanceof WriteConflict){
            assert e instanceof IOException: "Programmer error: WriteConflict should be an IOException";
            return (IOException)e;
        } else if(e instanceof ReadOnlyModificationException){
            assert e instanceof IOException: "Programmer error: ReadOnlyModificationException should be an IOException";
            return (IOException)e;
        } else if(e instanceof TransactionTimeoutException) {
            assert e instanceof IOException: "Programmer error: TransactionTimeoutException should be an IOException";
            return (IOException)e;
        } else if(e instanceof CannotCommitException) {
            assert e instanceof IOException: "Programmer error: CannotCommitException should be an IOException";
            return (IOException)e;
        } else if(e instanceof RetriesExhaustedWithDetailsException){
            RetriesExhaustedWithDetailsException rewde = (RetriesExhaustedWithDetailsException)e;
            for(Throwable c:rewde.getCauses()){
                if(c instanceof IOException){
                    return processRemoteException(c);
                }
            }
            return processRemoteException(rewde.getCause(0));
        }else if(e instanceof IOException) {
            return (IOException)e;
        } else
            return new IOException(e);
    }
}
