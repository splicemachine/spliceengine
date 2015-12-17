package com.splicemachine.si.impl;

import com.splicemachine.si.api.txn.lifecycle.TransactionTimeoutException;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MTransactionTimeout extends IOException implements TransactionTimeoutException{

    public MTransactionTimeout(){
    }

    public MTransactionTimeout(long txnId){
        super("Transaction "+txnId+" has timed out");
    }

    public MTransactionTimeout(String message){
        super(message);
    }

    public MTransactionTimeout(String message,Throwable cause){
        super(message,cause);
    }
}
