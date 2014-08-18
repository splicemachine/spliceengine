package com.splicemachine.si.impl;

import com.google.common.collect.Iterators;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.ByteSlice;

import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 8/14/14
 */
public abstract class AbstractTxnView implements TxnView {
    protected final long txnId;
    protected final long beginTimestamp;
    protected final Txn.IsolationLevel isolationLevel;

    protected AbstractTxnView(long txnId,
                              long beginTimestamp,
                              Txn.IsolationLevel isolationLevel) {
        this.txnId = txnId;
        this.beginTimestamp = beginTimestamp;
        this.isolationLevel = isolationLevel;
    }

    @Override
    public Txn.State getEffectiveState() {
        Txn.State currState = getState();
        if(currState== Txn.State.ROLLEDBACK) return currState; //if we are rolled back, then we were rolled back
        if(isDependent()){
            //if we are dependent, then defer to parent's status
            return getParentTxnView().getEffectiveState();
        }
        return currState;
    }

    @Override public Txn.IsolationLevel getIsolationLevel() { return isolationLevel; }
    @Override public long getTxnId() { return txnId; }
    @Override public long getBeginTimestamp() { return beginTimestamp; }


    @Override
    public long getEffectiveBeginTimestamp() {
        TxnView parent = getParentTxnView();
        if(parent!=null && !Txn.ROOT_TRANSACTION.equals(parent))
            return parent.getEffectiveBeginTimestamp();
        return beginTimestamp;
    }

    @Override
    public long getLastKeepAliveTimestamp() {
        return -1l;
    }

    @Override
    public TxnView getParentTxnView() {
        return null;
    }

    @Override public long getParentTxnId() { return getParentTxnView().getTxnId(); }

    @Override
    public Txn.State getState() {
        return null;
    }

    @Override
    public boolean allowsWrites() {
        return false;
    }

    	@Override
		public final boolean canSee(TxnView otherTxn) {
				assert otherTxn!=null: "Cannot access visibility semantics of a null transaction!";
				if(equals(otherTxn)) return true; //you can always see your own writes

				/*
				 * If otherTxn is a child of us, then we have one of two
				 * visibility rules--either READ UNCOMMITTED or READ COMMITTED. If
				 * you are in SNAPSHOT_ISOLATION, that is equivalent to READ_COMMITTED. Otherwise,
				 * a parent won't ever be able to see it's child's writes (violating the constraint
				 * that a child mimics the behavior of the parent).
				 *
				 * However, if otherTxn is a parent of us, then we can always see those writes.
				 */

				//look to see otherTxn is a parent (or grandparent, etc.)
				TxnView parent = getParentTxnView();
				while(parent!=null && parent.getTxnId()>=0){
						if(parent.equals(otherTxn)) {
								return parent.getEffectiveState() != Txn.State.ROLLEDBACK;
						}
						else
								parent = parent.getParentTxnView(); //keep going, it might be a grandparent
				}

				//look to see if we are a parent of otherTxn--if so, adjust the isolation leve
				parent = otherTxn;
				while(parent!=null && parent.getTxnId()>=0){
						if(equals(parent)){
								//we are a parent of this transaction
								Txn.IsolationLevel toCheck = isolationLevel;
								if(isolationLevel== Txn.IsolationLevel.SNAPSHOT_ISOLATION)
										toCheck = Txn.IsolationLevel.READ_COMMITTED;
								return toCheck.canSee(beginTimestamp,otherTxn,true);
						}else
								parent = parent.getParentTxnView();
				}

				//it's not a parent transaction of us, so default to visibility semantics
				return isolationLevel.canSee(getEffectiveBeginTimestamp(),otherTxn,false);
		}

    @Override
    public boolean isDependent() {
        return false;
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
    public ConflictType conflicts(TxnView otherTxn) {
/*
				 * There are two ways that a transaction does not conflict.
				 *
				 * 1. otherTxn.equals(this)
				 * 2. otherTxn is on a dependent hierarchical chain of this (e.g. otherTxn is a child,grandchild, etc)
				 * 3. this is on a dependent hierarchical chain of otherTxn
				 *
				 * otherwise, we conflict
				 */
        if(equals(otherTxn)) return ConflictType.NONE; //cannot conflict with ourself
        if(otherTxn.getEffectiveState()== Txn.State.ROLLEDBACK) return ConflictType.NONE; //cannot conflict with rolled back transactions

        //check to see if we are a parent of the other transaction--if so, this is a child conflict
        TxnView t = otherTxn;
        while(t!=null && t.getTxnId()>=0){
            if(equals(t)) return ConflictType.CHILD;
            t = t.getParentTxnView();
        }

        //check to see if the other transaction is a parent of us, in which case there is no conflict
        t = this;
        while(t!=null && t.getTxnId()>=0){
            if(otherTxn.equals(t)) return ConflictType.NONE;
            t = t.getParentTxnView();
        }

        if(otherTxn.getEffectiveState()== Txn.State.COMMITTED){
            return otherTxn.getEffectiveCommitTimestamp()> beginTimestamp ? ConflictType.SIBLING: ConflictType.NONE;
        }else{
            return ConflictType.SIBLING;
        }
    }

    @Override
		public Iterator<ByteSlice> getDestinationTables() {
				return Iterators.emptyIterator();
    }

    @Override
    public boolean descendsFrom(TxnView potentialParent) {
        TxnView t = this;
        while(t!=null && !t.equals(Txn.ROOT_TRANSACTION)){
            if(t.equals(potentialParent)) return true;
            else
                t = t.getParentTxnView();
        }
        return false;
    }

    @Override
		public boolean equals(Object o) {
				if (this == o) return true;
				if (!(o instanceof TxnView)) return false;

				TxnView that = (TxnView) o;

				return txnId == that.getTxnId();
		}

		@Override
		public int hashCode() {
				return (int) (txnId ^ (txnId >>> 32));
		}
}
