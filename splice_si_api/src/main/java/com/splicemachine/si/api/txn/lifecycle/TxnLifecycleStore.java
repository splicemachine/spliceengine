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

package com.splicemachine.si.api.txn.lifecycle;

import com.splicemachine.si.api.txn.TxnTimeTravelResult;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.Source;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/14/15
 */
public interface TxnLifecycleStore{

    void beginTransaction(TxnMessage.TxnInfo txn) throws IOException;

    void elevateTransaction(long txnId, byte[] destTable) throws IOException;

    long commitTransaction(long txnId) throws IOException;

    void rollbackTransaction(long txnId) throws IOException;

    void rollbackSubtransactions(long txnId, long[] subIds) throws IOException;

    boolean keepAlive(long txnId) throws IOException;

    TxnMessage.Txn getTransaction(long txnId) throws IOException;

    long[] getActiveTransactionIds(byte[] destTable, long startId, long endId) throws IOException;

    Source<TxnMessage.Txn> getActiveTransactions(byte[] destTable, long startId, long endId) throws IOException;

    void rollbackTransactionsAfter(long txnId) throws IOException;
    
    TxnMessage.Txn getOldTransaction(long txnId) throws IOException;

    TxnMessage.TaskId getTaskId(long txnId) throws IOException;

    Pair<Long, Long> getTxnAt(long ts) throws IOException;
}
