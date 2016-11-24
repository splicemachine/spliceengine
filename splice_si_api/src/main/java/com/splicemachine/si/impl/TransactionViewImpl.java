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

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

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
public class TransactionViewImpl extends BaseTransaction {
    private static Logger LOG = Logger.getLogger(TransactionViewImpl.class);

    private TxnView txn;

    public TransactionViewImpl(String transName, TxnView txn) {
        SpliceLogUtils.trace(LOG, "Instantiating Splice transaction");
        this.transName = transName;
        this.state = BaseTransaction.ACTIVE;
        this.txn = txn;
    }

    @Override
    public boolean allowsWrites(){
        return txn.allowsWrites();
    }

    @Override
    public void commit() throws IOException {
        ExceptionFactory ef =SIDriver.driver().getExceptionFactory();
        throw new IOException(ef.cannotCommit("Cannot commit from SpliceTransactionView"));
    }

    @Override
    public void abort() throws IOException {
        throw new UnsupportedOperationException("Cannot abort from SpliceTransactionView");
    }

    @Override protected void clearState() { txn = null; }
    @Override public TxnView getTxnInformation() { return txn; }

    @Override
    public String getActiveStateTxIdString() {
        if(txn!=null)
            return txn.toString();
        else
            return null;
    }

    @Override
    public void setActiveState(boolean nested, boolean dependent, TxnView parentTxn,byte[] tableName) {
        assert state==ACTIVE: "Cannot have an inactive SpliceTransactionView";
        //otherwise, it's a no-op
    }

    @Override
    public void setActiveState(boolean nested, boolean dependent, TxnView parentTxn) {
        assert state==ACTIVE: "Cannot have an inactive SpliceTransactionView";
        //otherwise, it's a no-op
    }

    public void setTxn(Txn txn) {
        this.txn = txn;
    }
}
