package com.splicemachine.derby.impl.store.access;

import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.CannotCommitException;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
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
    public LogInstant commit() throws StandardException {
        throw Exceptions.parseException(new CannotCommitException("Cannot commit from SpliceTransactionView"));
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
