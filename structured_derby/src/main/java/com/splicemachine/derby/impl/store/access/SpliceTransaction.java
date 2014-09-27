package com.splicemachine.derby.impl.store.access;

import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.si.api.TransactionLifecycle;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.locks.CompatibilitySpace;
import org.apache.derby.iapi.store.raw.log.LogInstant;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.log4j.Logger;

import java.io.IOException;

public class SpliceTransaction extends BaseSpliceTransaction {
    private static Logger LOG = Logger.getLogger(SpliceTransaction.class);

		private Txn txn;

		public SpliceTransaction(CompatibilitySpace compatibilitySpace,
														 DataValueFactory dataValueFactory,
														 String transName) {
				SpliceLogUtils.trace(LOG,"Instantiating Splice transaction");
				this.compatibilitySpace = compatibilitySpace;
				this.dataValueFactory = dataValueFactory;
				this.transName = transName;
				this.state = IDLE;
		}

		public SpliceTransaction(CompatibilitySpace compatibilitySpace, DataValueFactory dataValueFactory,
														 String transName, Txn txn) {
				SpliceLogUtils.trace(LOG,"Instantiating Splice transaction");
				this.compatibilitySpace = compatibilitySpace;
				this.dataValueFactory = dataValueFactory;
				this.transName = transName;
				this.state = ACTIVE;
				this.txn = txn;
		}

		public LogInstant commit() throws StandardException {
				if(LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG, "commit, state=" + state + " for transaction " + txn);

				if (state == IDLE) {
						if(LOG.isTraceEnabled())
								SpliceLogUtils.trace(LOG, "The transaction is in idle state and there is nothing to commit, transID=" + txn);
						return null;
				}

				if (state == CLOSED) {
						throw StandardException.newException("Transaction has already closed and cannot commit again");
				}

				try {
						txn.commit();
						state = IDLE;
				} catch (Exception e) {
            throw Exceptions.parseException(e);
				}
				return null;
		}


		public void abort() throws StandardException {
				SpliceLogUtils.debug(LOG,"abort");
				try {
						if (state !=ACTIVE)
								return;
						txn.rollback();
						state = IDLE;
				} catch (Exception e) {
						throw StandardException.newException(e.getMessage(), e);
				}
		}



		public String getActiveStateTxIdString() {
				SpliceLogUtils.debug(LOG,"getActiveStateTxIdString");
				setActiveState(false, false, null);
				if(txn!=null)
						return txn.toString();
				else
						return null;
		}

    public Txn getActiveStateTxn() {
        setActiveState(false, false, null);
        if(txn!=null)
            return txn;
        else
            return null;
    }

		public final String getContextId() {
				SpliceTransactionContext tempxc = transContext;
				return (tempxc == null) ? null : tempxc.getIdName();
		}

    public final void setActiveState(boolean nested, boolean additive, TxnView parentTxn) {
        setActiveState(nested, additive, parentTxn,null);
    }

		public final void setActiveState(boolean nested, boolean additive, TxnView parentTxn,byte[] table) {
				if (state == IDLE) {
            try {
                synchronized(this) {
//										if(nested){
                    TxnLifecycleManager lifecycleManager = TransactionLifecycle.getLifecycleManager();
                    if(nested)
                        txn = lifecycleManager.beginChildTransaction(parentTxn,parentTxn.getIsolationLevel(), additive,table);
                    else
                        txn = lifecycleManager.beginTransaction();
//										}
                    state = ACTIVE;
                }
            } catch (Exception e) {
                SpliceLogUtils.logAndThrowRuntime(LOG, e);
            }
        }
    }

//		private TransactionId generateTransactionId(boolean nested, boolean dependent, String parentTransactionID, boolean allowWrites) throws IOException {
//				TransactionId result;
//				if (nested) {
//						final TransactionId parentTransaction = transactor.transactionIdFromString(parentTransactionID);
//						result = transactor.beginChildTransaction(parentTransaction, dependent, allowWrites);
//				} else {
//						result = transactor.beginTransaction();
//				}
//				return result;
//		}

		public int getTransactionStatus() {
				return state;
		}

		public Txn getTxn() {
				return txn;
		}

		public void setTxn(Txn txn) { this.txn = txn; }

    public Txn elevate(byte[] writeTable) throws StandardException {
        TxnView parent = txn!=null? txn.getParentTxnView(): Txn.ROOT_TRANSACTION;
        setActiveState(false, false, parent);
        if(!txn.allowsWrites()){
            try {
                txn = txn.elevateToWritable(writeTable);
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
        }
        return txn;
    }

    @Override protected void clearState() { txn = null; }

    @Override
    public TxnView getTxnInformation() {
        return txn;
    }

    @Override
    public String toString() {
        String s = "SpliceTransaction(";
        if(state==IDLE)
            s += "IDLE";
        else if(state==ACTIVE)
            s += "ACTIVE";
        else
            s += "CLOSED";
        s+=","+txn+")";
        return s;
    }

    public boolean allowsWrites() {
        return txn!=null && txn.allowsWrites();
    }
}
