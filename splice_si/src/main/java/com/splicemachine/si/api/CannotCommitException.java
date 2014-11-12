package com.splicemachine.si.api;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
public class CannotCommitException extends DoNotRetryIOException {
    private long txnId = -1;

		public CannotCommitException(long txnId,Txn.State actualState){
        super(String.format("[%d]Transaction %d cannot be committed--it is in the %s state",txnId,txnId,actualState));
		}

    public CannotCommitException(String message){
        super(message);
    }

    public long getTxnId(){
        if(txnId<0)
            txnId = parseTxn(getMessage());
        return txnId;
    }

    private long parseTxn(String message) {
        int openIndex = message.indexOf("[");
        int closeIndex = message.indexOf("]");
        String txnNum = message.substring(openIndex+1,closeIndex);
        return Long.parseLong(txnNum);
    }

    public Txn.State getActualState() {
        String message = getMessage();
        int startIndex = message.indexOf("the")+4;
        int endIndex = message.indexOf(" state");
        return Txn.State.fromString(message.substring(startIndex,endIndex));
    }
}
