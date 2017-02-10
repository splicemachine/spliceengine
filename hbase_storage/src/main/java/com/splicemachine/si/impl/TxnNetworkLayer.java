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

import com.splicemachine.si.coprocessor.TxnMessage;

import java.io.IOException;
import java.util.Collection;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public interface TxnNetworkLayer extends AutoCloseable{

    void beginTransaction(byte[] rowKey,TxnMessage.TxnInfo txnInfo) throws IOException;

    TxnMessage.ActionResponse lifecycleAction(byte[] rowKey,TxnMessage.TxnLifecycleMessage lifecycleMessage) throws IOException;

    void elevate(byte[] rowKey,TxnMessage.ElevateRequest elevateRequest) throws IOException;

    long[] getActiveTxnIds(TxnMessage.ActiveTxnRequest request) throws IOException;

    Collection<TxnMessage.ActiveTxnResponse> getActiveTxns(TxnMessage.ActiveTxnRequest request) throws IOException;

    TxnMessage.Txn getTxn(byte[] rowKey,TxnMessage.TxnRequest request) throws IOException;

    void close() throws IOException;
}
