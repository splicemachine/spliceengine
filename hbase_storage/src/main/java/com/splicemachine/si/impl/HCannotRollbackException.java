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

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class HCannotRollbackException extends DoNotRetryIOException {
    private long txnId = -1;

    public HCannotRollbackException(long txnId, String message) {
        super(String.format("[%d]Transaction %d cannot be rolled back--%s", txnId, txnId, message));
    }

    public HCannotRollbackException(String message) {
        super(message);
    }

    public long getTxnId() {
        if (txnId < 0)
            txnId = parseTxn(getMessage());
        return txnId;
    }

    private long parseTxn(String message) {
        int openIndex = message.indexOf("[");
        int closeIndex = message.indexOf("]");
        String txnNum = message.substring(openIndex + 1, closeIndex);
        return Long.parseLong(txnNum);
    }
}
