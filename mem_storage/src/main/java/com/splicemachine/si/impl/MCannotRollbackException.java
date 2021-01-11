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

import com.splicemachine.si.api.txn.lifecycle.CannotRollbackException;

import java.io.IOException;

public class MCannotRollbackException extends IOException implements CannotRollbackException {
    private long txnId = -1;
    private long originatingTxnId = -1;

    public MCannotRollbackException(long txnId, long originatingTxn, String message) {
        super(String.format("[%d](%d)Transaction %d cannot be rolled back by transaction %d--%s",
                            txnId, originatingTxn, txnId, originatingTxn, message));
        this.txnId = txnId;
        this.originatingTxnId = originatingTxn;
    }

    public MCannotRollbackException(String message) {
        super(message);
    }

    @Override
    public long getTxnId() {
        if(txnId<0)
            txnId=parseTxn(getMessage(), "[", "]");
        return txnId;
    }

    @Override
    public long getOriginatingTxnId() {
        if(originatingTxnId<0)
            originatingTxnId=parseTxn(getMessage(), "(", ")");
        return originatingTxnId;
    }

    private long parseTxn(String message, String start, String end){
        if(message == null) {
            return -1;
        }
        int openIndex=message.indexOf(start);
        int closeIndex=message.indexOf(end);
        if(0 < openIndex && openIndex < closeIndex && closeIndex <= message.length()) {
            String txnNum = message.substring(openIndex + 1, closeIndex);
            return Long.parseLong(txnNum);
        }
        return -1;
    }
}
