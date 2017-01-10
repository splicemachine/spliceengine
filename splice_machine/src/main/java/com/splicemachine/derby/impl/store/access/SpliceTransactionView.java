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

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.Txn;
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
                             String transName, Txn txn) {
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

    @Override public Txn getTxnInformation() { return transaction.getTxnInformation(); }

    @Override
    public String getActiveStateTxIdString() {
        return transaction.getActiveStateTxIdString();
    }

    @Override
    public void setActiveState(boolean nested, boolean dependent, Txn parentTxn) {
        transaction.setActiveState(nested, dependent, parentTxn);
    }

    public void setTxn(Txn txn) {
        transaction.setTxn(txn);
    }
}
