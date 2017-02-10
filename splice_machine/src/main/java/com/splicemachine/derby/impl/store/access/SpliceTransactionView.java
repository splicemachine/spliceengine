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

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.TransactionViewImpl;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.locks.CompatibilitySpace;
import com.splicemachine.db.iapi.types.DataValueFactory;
import org.apache.log4j.Logger;

/**
 * A view-only representation of a Derby transaction.
 *
 * This should be used when we created the transaction on a separate
 * node than where we are marshalling it.
 *
 * This implementation CANNOT be committed, so don't try to use it for
 * that purpose.
 *
 * @author Scott Fines
 * Date: 8/14/14
 */
public class SpliceTransactionView extends BaseSpliceTransaction<TransactionViewImpl> {
    private static Logger LOG = Logger.getLogger(SpliceTransaction.class);

    public SpliceTransactionView(CompatibilitySpace compatibilitySpace,
    						 SpliceTransactionFactory spliceTransactionFactory,
    						 DataValueFactory dataValueFactory,
                             String transName, TxnView txn) {
        SpliceLogUtils.trace(LOG, "Instantiating Splice transaction");
        this.compatibilitySpace = compatibilitySpace;
		this.spliceTransactionFactory = spliceTransactionFactory;
        this.dataValueFactory = dataValueFactory;
        this.transaction = new TransactionViewImpl(transName, txn);
    }

    @Override
    public boolean allowsWrites(){
        return transaction.allowsWrites();
    }

    @Override
    public void commit() throws StandardException {
        ExceptionFactory ef =SIDriver.driver().getExceptionFactory();
        throw Exceptions.parseException(ef.cannotCommit("Cannot commit from SpliceTransactionView"));
    }

    @Override
    public void abort() throws StandardException {
        throw new UnsupportedOperationException("Cannot abort from SpliceTransactionView");
    }

    @Override public TxnView getTxnInformation() { return transaction.getTxnInformation(); }

    @Override
    public String getActiveStateTxIdString() {
        return transaction.getActiveStateTxIdString();
    }

    @Override
    public void setActiveState(boolean nested, boolean dependent, TxnView parentTxn) {
        transaction.setActiveState(nested, dependent, parentTxn);
    }

    public void setTxn(Txn txn) {
        transaction.setTxn(txn);
    }
}
