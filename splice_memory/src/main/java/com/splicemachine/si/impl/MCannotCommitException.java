package com.splicemachine.si.impl;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.lifecycle.CannotCommitException;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MCannotCommitException extends IOException implements CannotCommitException{
    private long txnId=-1;

    public MCannotCommitException(long txnId,Txn.State actualState){
        super(String.format("[%d]Transaction %d cannot be committed--it is in the %s state",txnId,txnId,actualState));
    }

    public MCannotCommitException(String message){
        super(message);
    }

    public long getTxnId(){
        if(txnId<0)
            txnId=parseTxn(getMessage());
        return txnId;
    }

    private long parseTxn(String message){
        int openIndex=message.indexOf("[");
        int closeIndex=message.indexOf("]");
        String txnNum=message.substring(openIndex+1,closeIndex);
        return Long.parseLong(txnNum);
    }

    public Txn.State getActualState(){
        String message=getMessage();
        int startIndex=message.indexOf("the")+4;
        int endIndex=message.indexOf(" state");
        return Txn.State.fromString(message.substring(startIndex,endIndex));
    }
}
