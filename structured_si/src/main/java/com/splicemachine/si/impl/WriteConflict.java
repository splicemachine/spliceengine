package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Exception indicating that a transaction failed because it tried to write to data that was modified since the
 * transaction began (i.e. the transaction collided with another).
 */
public class WriteConflict extends DoNotRetryIOException {
    private long txn1 = -1l;
    private long txn2 = -1l;

    /**
     * Used for serialization, DO NOT USE
     */
    @Deprecated
    public WriteConflict() { super(); }

    public WriteConflict(long txn1,long txn2){
        this(String.format("[%1$d,%2$d] Write conflict detected between transactions %1$d and %2$d",txn1,txn2));
        this.txn1 = txn1;
        this.txn2 = txn2;
    }
    /*
     * We are forced to put in an ugly hack to make sure that we
     * can transmit the transaction ids properly--and therefore
     * can present a useful error message to the user.
     *
     * Because we don't get serialized in a meaningful way, we have
     * to store all of our information in the error message,
     * which makes it vitally important that the error message be
     * properly formatted for easy reading.
     *
     * As a result, we remove the ability to create WriteConflict
     * exceptions from string messages, because we want to validate
     * that the message is created properly
     */

    public static WriteConflict fromString(String message){
        long txn1 = parseTxn1(message); //will throw an error if it can't be parsed
        long txn2 = parseTxn2(message); //will throw an error if it can't be parsed

        return new WriteConflict(message);
    }

    public static WriteConflict fromThrowable(String message,Throwable baseCause){
        long txn1 = parseTxn1(message); //will throw an error if it can't be parsed
        long txn2 = parseTxn2(message); //will throw an error if it can't be parsed

        return new WriteConflict(message,baseCause);
    }


    public WriteConflict(String message) { super(message); }
    public WriteConflict(String message, Throwable cause) { super(message, cause);}

//    @Override
//    public String getMessage() {
//        String message = super.getMessage();
//        return message.substring(message.indexOf("]")+1); //strip internal stuff from our message
//    }

    public long getFirstTransaction() {
        if(txn1<0)
            txn1 = parseTxn1(getInternalMessage());

        return txn1;
    }
    public long getSecondTransaction(){
        if(txn2<0)
            txn2 = parseTxn2(getInternalMessage());
        return txn2;
    }

    /*****************************************************************************************************************/
    /*private helper methods*/
    private String getInternalMessage(){
        return super.getMessage();
    }

    private static long parseTxn1(String message) {
        int commaIndex = message.indexOf(",");
        int openIndex = message.indexOf("[");
        String txnNum = message.substring(openIndex+1,commaIndex);
        return Long.parseLong(txnNum);
    }

    private static long parseTxn2(String message) {
        int commaIndex = message.indexOf(",")+1;
        int closeIndex = message.indexOf("]");
        String txnNum = message.substring(commaIndex,closeIndex);
        return Long.parseLong(txnNum);
    }
}
