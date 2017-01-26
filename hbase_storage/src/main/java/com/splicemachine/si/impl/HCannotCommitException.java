/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
        super(String.format("[%d]Transaction %d cannot be committed--it is in the %s state",txnId,txnId,actualState));
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
