/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.impl;

import com.splicemachine.si.api.txn.WriteConflict;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Exception indicating that a transaction failed because it tried to write to data that was modified since the
 * transaction began (i.e. the transaction collided with another).
 */
public class HWriteConflict extends DoNotRetryIOException implements WriteConflict{
    private long txn1 = -1l;
    private long txn2 = -1l;

    /**
     * Used for serialization, DO NOT USE
     */
    @Deprecated
    public HWriteConflict() { super(); }

    public HWriteConflict(long txn1,long txn2){
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
     * As a result, we remove the ability to create HWriteConflict
     * exceptions from string messages, because we want to validate
     * that the message is created properly
     */

    @SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE",justification = "intentional")
    public static HWriteConflict fromString(String message){
        long txn1 = parseTxn1(message); //will throw an error if it can't be parsed
        long txn2 = parseTxn2(message); //will throw an error if it can't be parsed

        return new HWriteConflict(message);
    }

    @SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE",justification = "intentional")
    public static HWriteConflict fromThrowable(String message,Throwable baseCause){
        long txn1 = parseTxn1(message); //will throw an error if it can't be parsed
        long txn2 = parseTxn2(message); //will throw an error if it can't be parsed

        return new HWriteConflict(message,baseCause);
    }


    public HWriteConflict(String message) { super(message); }
    public HWriteConflict(String message,Throwable cause) { super(message, cause);}

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
