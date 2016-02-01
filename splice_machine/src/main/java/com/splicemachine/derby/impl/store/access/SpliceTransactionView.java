package com.splicemachine.derby.impl.store.access;

import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.locks.CompatibilitySpace;
import com.splicemachine.db.iapi.store.raw.log.LogInstant;
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
public class SpliceTransactionView extends BaseSpliceTransaction {
    private static Logger LOG = Logger.getLogger(SpliceTransaction.class);

    private TxnView txn;

    public SpliceTransactionView(CompatibilitySpace compatibilitySpace,
    						 SpliceTransactionFactory spliceTransactionFactory,
    						 DataValueFactory dataValueFactory,
                             String transName, TxnView txn) {
        SpliceLogUtils.trace(LOG, "Instantiating Splice transaction");
        this.compatibilitySpace = compatibilitySpace;
		this.spliceTransactionFactory = spliceTransactionFactory;
        this.dataValueFactory = dataValueFactory;
        this.transName = transName;
        this.state = SpliceTransaction.ACTIVE;
        this.txn = txn;
    }

    @Override
    public boolean allowsWrites(){
        return txn.allowsWrites();
    }

    @Override
    public LogInstant commit() throws StandardException {
        ExceptionFactory ef =SIDriver.driver().getExceptionFactory();
        throw Exceptions.parseException(ef.cannotCommit("Cannot commit from SpliceTransactionView"));
    }

    @Override
    public void abort() throws StandardException {
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
