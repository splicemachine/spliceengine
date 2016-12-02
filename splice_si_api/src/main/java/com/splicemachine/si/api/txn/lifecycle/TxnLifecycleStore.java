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

package com.splicemachine.si.api.txn.lifecycle;

import com.splicemachine.si.coprocessor.TxnMessage;
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
}
