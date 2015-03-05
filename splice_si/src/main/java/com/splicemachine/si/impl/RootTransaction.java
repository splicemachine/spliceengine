package com.splicemachine.si.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

import com.google.common.collect.Iterators;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.ByteSlice;

public class RootTransaction implements Txn {
	public RootTransaction() {
		
	}
        @Override 
        public String toString(){ 
        	return "ROOT"; 
        }
        @Override 
        public Iterator<ByteSlice> getDestinationTables() { 
        	return Iterators.emptyIterator();
        }
        @Override 
        public boolean descendsFrom(TxnView potentialParent) { 
        	return false; 
        }
        @Override 
        public State getEffectiveState() { 
        	return State.ACTIVE; 
        }
        @Override 
        public IsolationLevel getIsolationLevel() { 
        	return IsolationLevel.SNAPSHOT_ISOLATION; 
        }
        @Override 
        public long getTxnId() { 
        	return -1l; 
        }
        @Override 
        public long getBeginTimestamp() { 
        	return 0; 
        }
        @Override public 
        long getCommitTimestamp() { 
        	return -1l; 
        }
        @Override public 
        long getEffectiveCommitTimestamp() { 
        	return -1l; 
        }
        @Override public 
        long getEffectiveBeginTimestamp() { 
        	return 0; 
        }

        @Override public long getLastKeepAliveTimestamp() { return -1l; }

        @Override public TxnView getParentTxnView() { return null; }
        @Override public long getParentTxnId() { return -1l; }

        @Override public State getState() { return State.ACTIVE; }
        @Override public boolean allowsWrites() { return true; }
		@Override public String getSavePointName() { return null; }

        @Override
        public void commit() throws IOException {
            throw new UnsupportedOperationException("Cannot commit the root transaction");
        }

        @Override
        public void rollback() throws IOException {
            throw new UnsupportedOperationException("Cannot rollback the root transaction");
        }

        @Override
        public Txn elevateToWritable(byte[] writeTable) throws IOException {
            throw new UnsupportedOperationException("Cannot elevate the root transaction");
        }

        @Override public boolean canSee(TxnView otherTxn) { return false; }

        @Override public boolean isAdditive() { return false; }

        @Override public long getGlobalCommitTimestamp() { return -1l; }

        @Override
        public ConflictType conflicts(TxnView otherTxn) {
            return ConflictType.CHILD; //every transaction is a child of this
        }
		@Override
		public void readExternal(ObjectInput in) throws IOException,
				ClassNotFoundException {
			// No-Op
		}
		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			// No-Op
		}

		@Override
		public void setSavePointName(String savePointName) {
			// No-Op
		}
}
