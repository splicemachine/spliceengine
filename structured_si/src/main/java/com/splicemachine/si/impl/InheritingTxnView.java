package com.splicemachine.si.impl;

import com.google.common.collect.Iterators;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.ByteSlice;

import java.util.Iterator;

/**
 * Transaction which is partially constructed, but which looks up values in order
 * to complete itself when necessary.
 *
 * Useful primarily for child transactions during initial construction, in order
 * to populate default values.
 *
 * @author Scott Fines
 * Date: 6/19/14
 */
public class InheritingTxnView extends AbstractTxnView {
		private final boolean hasAdditive;
		private final boolean isAdditive;

		private final TxnView parentTxn;
		private final long commitTimestamp;
		private final Txn.State state;
		private final boolean allowWrites;
		private final boolean hasAllowWrites;
		private long globalCommitTimestamp;

		private final Iterator<ByteSlice> destinationTables;

    private final long lastKaTime;

    public InheritingTxnView(TxnView parentTxn,
                             long txnId,long beginTimestamp,
                             boolean allowWrites,
                             Txn.IsolationLevel isolationLevel,
                             Txn.State state){
        this(parentTxn,
                txnId,
                beginTimestamp,
                isolationLevel,
                false,false,
                true,allowWrites,-1l,-1l,state);
    }

		public InheritingTxnView(TxnView parentTxn,
														 long txnId,long beginTimestamp,
														 Txn.IsolationLevel isolationLevel,
														 Txn.State state){
				this(parentTxn,
								txnId,
								beginTimestamp,
								isolationLevel,
                false,false,false,false,-1l,-1l,state);
		}

		public InheritingTxnView(TxnView parentTxn,
														 long txnId, long beginTimestamp,
														 Txn.IsolationLevel isolationLevel,
														 boolean hasAdditive, boolean isAdditive,
														 boolean hasAllowWrites,boolean allowWrites,
														 long commitTimestamp,long globalCommitTimestamp,
														 Txn.State state) {
			this(parentTxn, txnId, beginTimestamp, isolationLevel,
							hasAdditive, isAdditive,
							hasAllowWrites, allowWrites,
							commitTimestamp, globalCommitTimestamp,
							state, Iterators.<ByteSlice>emptyIterator());
		}

    public InheritingTxnView(TxnView parentTxn,
                             long txnId, long beginTimestamp,
                             Txn.IsolationLevel isolationLevel,
                             boolean hasAdditive, boolean isAdditive,
                             boolean hasAllowWrites,boolean allowWrites,
                             long commitTimestamp,long globalCommitTimestamp,
                             Txn.State state,
                             Iterator<ByteSlice> destinationTables) {
        this(parentTxn, txnId, beginTimestamp, isolationLevel,
                hasAdditive, isAdditive,
                hasAllowWrites, allowWrites,
                commitTimestamp, globalCommitTimestamp,
                state,destinationTables,-1l);
    }

		public InheritingTxnView(TxnView parentTxn,
														 long txnId, long beginTimestamp,
														 Txn.IsolationLevel isolationLevel,
														 boolean hasAdditive, boolean isAdditive,
														 boolean hasAllowWrites,boolean allowWrites,
														 long commitTimestamp,long globalCommitTimestamp,
														 Txn.State state,
														 Iterator<ByteSlice> destinationTables,
                             long lastKaTime) {
				super(txnId, beginTimestamp, isolationLevel);
				this.hasAdditive = hasAdditive;
				this.isAdditive = isAdditive;
				this.parentTxn = parentTxn;
				this.commitTimestamp = commitTimestamp;
				this.state = state;
				this.allowWrites = allowWrites;
				this.hasAllowWrites = hasAllowWrites;
				this.globalCommitTimestamp = globalCommitTimestamp;
				this.destinationTables = destinationTables;
        this.lastKaTime = lastKaTime;
		}

    @Override public long getLastKeepAliveTimestamp() { return lastKaTime; }

    @Override
		public Iterator<ByteSlice> getDestinationTables() {
				return destinationTables;
		}

    @Override
		public boolean isAdditive() {
				if(hasAdditive) return isAdditive;
				return parentTxn.isAdditive();
		}

		@Override
		public long getGlobalCommitTimestamp() {
				if(globalCommitTimestamp==-1l) return parentTxn.getGlobalCommitTimestamp();
				return globalCommitTimestamp;
		}

		@Override
		public Txn.IsolationLevel getIsolationLevel() {
				if(isolationLevel!=null) return isolationLevel;
				return parentTxn.getIsolationLevel();
		}

		@Override public long getCommitTimestamp() { return commitTimestamp; }

    @Override
    public TxnView getParentTxnView() {
        return parentTxn;
    }

    @Override public Txn.State getState() { return state; }

		@Override
		public long getEffectiveCommitTimestamp() {
				if(globalCommitTimestamp>=0) return globalCommitTimestamp;
        if(Txn.ROOT_TRANSACTION.equals(parentTxn)) {
            globalCommitTimestamp = commitTimestamp;
        }else{
            globalCommitTimestamp = parentTxn.getEffectiveCommitTimestamp();
        }
        return globalCommitTimestamp;
		}

		@Override
		public boolean allowsWrites() {
				if(hasAllowWrites) return allowWrites;
				return parentTxn.allowsWrites();
		}
}
