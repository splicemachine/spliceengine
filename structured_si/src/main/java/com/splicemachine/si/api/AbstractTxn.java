package com.splicemachine.si.api;

import com.splicemachine.si.impl.ConflictType;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
public abstract class AbstractTxn implements Txn{
		protected final long txnId;
		protected final long beginTimestamp;
		protected final IsolationLevel isolationLevel;

		protected AbstractTxn(long txnId,
													long beginTimestamp,
													IsolationLevel isolationLevel) {
				this.txnId = txnId;
				this.beginTimestamp = beginTimestamp;
				this.isolationLevel = isolationLevel;
		}

		@Override
		public Collection<byte[]> getDestinationTables() {
				return Collections.emptyList();
		}

		@Override
		public boolean descendsFrom(Txn potentialParent) {
				Txn t = this;
				while(t!=null && !t.equals(Txn.ROOT_TRANSACTION)){
						if(t.equals(potentialParent)) return true;
						else
								t = t.getParentTransaction();
				}
				return false;
		}

		@Override
		public State getEffectiveState() {
				State currState = getState();
				if(currState==State.ROLLEDBACK) return currState; //if we are rolled back, then we were rolled back
				if(isDependent()){
						//if we are dependent, then defer to parent's status
						return getParentTransaction().getEffectiveState();
				}
				return currState;
		}

		@Override public IsolationLevel getIsolationLevel() { return isolationLevel; }

		@Override public long getTxnId() { return txnId; }

		@Override public long getBeginTimestamp() { return beginTimestamp; }

		@Override
		public final boolean canSee(Txn otherTxn) {
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
				Txn parent = getParentTransaction();
				while(parent!=null && parent.getTxnId()>=0){
						if(parent.equals(otherTxn)) {
								return parent.getEffectiveState() != State.ROLLEDBACK;
						}
						else
								parent = parent.getParentTransaction(); //keep going, it might be a grandparent
				}

				//look to see if we are a parent of otherTxn--if so, adjust the isolation leve
				parent = otherTxn;
				while(parent!=null && parent.getTxnId()>=0){
						if(equals(parent)){
								//we are a parent of this transaction
								IsolationLevel toCheck = isolationLevel;
								if(isolationLevel==IsolationLevel.SNAPSHOT_ISOLATION)
										toCheck = IsolationLevel.READ_COMMITTED;
								return toCheck.canSee(beginTimestamp,otherTxn,true);
						}else
								parent = parent.getParentTransaction();
				}

				//it's not a parent transaction of us, so default to visibility semantics
				return isolationLevel.canSee(getEffectiveBeginTimestamp(),otherTxn,false);
		}

		public long getEffectiveBeginTimestamp() {
				Txn parent = getParentTransaction();
				if(parent!=null && !Txn.ROOT_TRANSACTION.equals(parent))
						return parent.getEffectiveBeginTimestamp();
				return beginTimestamp;
		}

		@Override
		public ConflictType conflicts(Txn otherTxn) {
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
				if(otherTxn.getEffectiveState()==State.ROLLEDBACK) return ConflictType.NONE; //cannot conflict with rolled back transactions

				//check to see if we are a parent of the other transaction--if so, this is a child conflict
				Txn t = otherTxn;
				while(t!=null && t.getTxnId()>=0){
						if(equals(t)) return ConflictType.CHILD;
						t = t.getParentTransaction();
				}

				//check to see if the other transaction is a parent of us, in which case there is no conflict
				t = this;
				while(t!=null && t.getTxnId()>=0){
						if(otherTxn.equals(t)) return ConflictType.NONE;
						t = t.getParentTransaction();
				}

				if(otherTxn.getEffectiveState()==State.COMMITTED){
						return otherTxn.getEffectiveCommitTimestamp()> beginTimestamp ? ConflictType.SIBLING: ConflictType.NONE;
				}else{
						return ConflictType.SIBLING;
				}
		}

		@Override
		public boolean equals(Object o) {
				if (this == o) return true;
				if (!(o instanceof Txn)) return false;

				Txn that = (Txn) o;

				return txnId == that.getTxnId();
		}

		@Override
		public int hashCode() {
				return (int) (txnId ^ (txnId >>> 32));
		}
}
