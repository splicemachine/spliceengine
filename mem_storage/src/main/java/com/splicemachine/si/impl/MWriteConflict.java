/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.impl;


import com.splicemachine.si.api.txn.WriteConflict;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Exception indicating that a transaction failed because it tried to write to data that was modified since the
 * transaction began (i.e. the transaction collided with another).
 */
@SuppressFBWarnings("DLS_DEAD_LOCAL_STORE") //we use local store to make sure the messages parse properly
public class MWriteConflict extends IOException implements WriteConflict{
    private long txn1 = -1l;
    private long txn2 = -1l;

    /**
     * Used for serialization, DO NOT USE
     */
    @Deprecated
    public MWriteConflict() { super(); }

    public MWriteConflict(long txn1,long txn2){
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

    public static MWriteConflict fromString(String message){
        long txn1 = parseTxn1(message); //will throw an error if it can't be parsed
        long txn2 = parseTxn2(message); //will throw an error if it can't be parsed

        return new MWriteConflict(message);
    }

    public static MWriteConflict fromThrowable(String message,Throwable baseCause){
        long txn1 = parseTxn1(message); //will throw an error if it can't be parsed
        long txn2 = parseTxn2(message); //will throw an error if it can't be parsed

        return new MWriteConflict(message,baseCause);
    }


    public MWriteConflict(String message) { super(message); }
    public MWriteConflict(String message, Throwable cause) { super(message, cause);}

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
