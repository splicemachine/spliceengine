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

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.lifecycle.CannotCommitException;
import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
public class HCannotCommitException extends DoNotRetryIOException implements CannotCommitException{
    private long txnId=-1;

    public HCannotCommitException(long txnId,Txn.State actualState){
        super(String.format("[%d]Txn %d cannot be committed--it is in the %s state",txnId,txnId,actualState));
    }

    public HCannotCommitException(String message){
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
