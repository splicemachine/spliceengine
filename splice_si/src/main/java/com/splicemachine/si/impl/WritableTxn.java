package com.splicemachine.si.impl;

import com.splicemachine.si.api.CannotCommitException;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SliceIterator;
import com.splicemachine.utils.SpliceLogUtils;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.log4j.Logger;

/**
 * Represents a Transaction.
 *
 * @author Scott Fines
 * Date: 6/18/14
 */
public class WritableTxn extends AbstractTxn {

		private static final Logger LOG = Logger.getLogger(WritableTxn.class);
		private volatile TxnView parentTxn;
		private volatile long commitTimestamp = -1l;
		private volatile long globalCommitTimestamp = -1l;
		private TxnLifecycleManager tc;
		private boolean isAdditive;
		private volatile State state = State.ACTIVE;
		private Set<byte[]> tableWrites = new CopyOnWriteArraySet<byte[]>();

		public WritableTxn() {
			
		}
		
		public WritableTxn(long txnId,
                       long beginTimestamp,
                       IsolationLevel isolationLevel,
                       TxnView parentTxn,
                       TxnLifecycleManager tc,
                       boolean isAdditive) {
			this(txnId, beginTimestamp, isolationLevel, parentTxn, tc, isAdditive,null);
		}
		public WritableTxn(long txnId,
                       long beginTimestamp,
                       IsolationLevel isolationLevel,
                       TxnView parentTxn,
                       TxnLifecycleManager tc,
                       boolean isAdditive,
                       byte[] destinationTable) {
				super(txnId, beginTimestamp, isolationLevel);
				this.parentTxn = parentTxn;
				this.tc = tc;
				this.isAdditive = isAdditive;

				if(destinationTable!=null)
						this.tableWrites.add(destinationTable);
		}

		public WritableTxn(Txn txn,TxnLifecycleManager tc, byte[] destinationTable) {
				super(txn.getTxnId(),txn.getBeginTimestamp(),txn.getIsolationLevel());
				this.parentTxn = txn.getParentTxnView();
				this.tc = tc;
				this.isAdditive = txn.isAdditive();
				this.savePointName = txn.getSavePointName();
				if(destinationTable!=null)
						this.tableWrites.add(destinationTable);
		}

    @Override public boolean isAdditive() { return isAdditive; }

		@Override
		public long getGlobalCommitTimestamp() {
				if(globalCommitTimestamp<0) return parentTxn.getGlobalCommitTimestamp();
				return globalCommitTimestamp;
		}

		@Override
		public long getCommitTimestamp() {
				return commitTimestamp;
		}

		@Override
		public long getEffectiveCommitTimestamp() {
        if(state==State.ROLLEDBACK) return -1l;
				if(globalCommitTimestamp>=0) return globalCommitTimestamp;
        if(Txn.ROOT_TRANSACTION.equals(parentTxn))
            globalCommitTimestamp = commitTimestamp;
        else
						globalCommitTimestamp = parentTxn.getEffectiveCommitTimestamp();

        return globalCommitTimestamp;
		}

    @Override public TxnView getParentTxnView() { return parentTxn; }

    @Override
		public State getState() {
				return state;
		}

		@Override
		public void commit() throws IOException {
				if(LOG.isTraceEnabled())
					SpliceLogUtils.trace(LOG,"Before commit: txn=%s",this);
				switch (state){
						case COMMITTED:
								return;
						case ROLLEDBACK:
								throw new CannotCommitException(txnId,state);
				}
				synchronized (this){
						//double-checked locking for efficiency--usually not needed
						switch (state){
								case COMMITTED:
										return;
								case ROLLEDBACK:
										throw new CannotCommitException(txnId,state);
						}
						commitTimestamp = tc.commit(txnId);
						state = State.COMMITTED;
				}
				if(LOG.isTraceEnabled())
					SpliceLogUtils.trace(LOG,"After commit: txn=%s,commitTimestamp=%s",this,commitTimestamp);
		}

		@Override
		public void rollback() throws IOException {
				if(LOG.isTraceEnabled())
					SpliceLogUtils.trace(LOG,"Before rollback: txn=%s",this);
				switch(state){
						case COMMITTED://don't need to rollback
						case ROLLEDBACK:
								return;
				}

				synchronized (this){
						switch(state){
								case COMMITTED://don't need to rollback
								case ROLLEDBACK:
										return;
						}

						tc.rollback(txnId);
						state = State.ROLLEDBACK;
				}
				if(LOG.isTraceEnabled())
					SpliceLogUtils.trace(LOG,"After rollback: txn=%s",this);
		}

		@Override public boolean allowsWrites() { return true; }

		@Override
		public Txn elevateToWritable(byte[] writeTable) throws IOException {
				assert state==State.ACTIVE: "Cannot elevate an inactive transaction: "+ state;
				if(LOG.isTraceEnabled())
					SpliceLogUtils.trace(LOG,"Before elevateToWritable: txn=%s,writeTable=%s",this,writeTable);
				if(tableWrites.add(writeTable)){
						tc.elevateTransaction(this,writeTable);
				}
				if(LOG.isTraceEnabled())
					SpliceLogUtils.trace(LOG,"After elevateToWritable: txn=%s",this);
				return this;
		}

		@Override
		public Iterator<ByteSlice> getDestinationTables() {
        return new SliceIterator(tableWrites.iterator());
		}
}
