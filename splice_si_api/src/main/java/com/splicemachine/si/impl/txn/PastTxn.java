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
 *
 */

package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;

import java.io.IOException;

public class PastTxn extends AbstractTxn {
    public PastTxn() {
    }

    public PastTxn(long txnId) {
        super(null, txnId, txnId, Txn.IsolationLevel.SNAPSHOT_ISOLATION);
    }

    @Override
    public void subRollback() {
        // ignore
    }

    @Override
    public void commit() throws IOException {
        // ignore
    }

    @Override
    public void rollback() throws IOException {
        // ignore
    }

    @Override
    public Txn elevateToWritable(byte[] writeTable) throws IOException {
        ExceptionFactory ef = SIDriver.driver().getExceptionFactory();
        throw new IOException(ef.readOnlyModification("Txn "+txnId+" is a past transaction, it's read only"));
    }

    @Override
    public long getCommitTimestamp() {
        return 0;
    }

    @Override
    public boolean isAdditive() {
        return false;
    }

    @Override
    public long getGlobalCommitTimestamp() {
        return 0;
    }

    @Override
    public boolean equivalent(TxnView o) {
        return false;
    }

    @Override
    public TxnView getParentTxnView() {
        return Txn.ROOT_TRANSACTION;
    }
}
