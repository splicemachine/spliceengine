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
    public long getEffectiveCommitTimestamp() {
        long gCTs = getGlobalCommitTimestamp();
        if(gCTs>0) return gCTs;
        TxnView pTxn = getParentTxnView();
        if(Txn.ROOT_TRANSACTION.equals(pTxn)) return getCommitTimestamp();
        else return pTxn.getEffectiveCommitTimestamp();
    }

    @Override
    public Txn.State getEffectiveState() {
        Txn.State currState = getState();
        if(currState== Txn.State.ROLLEDBACK) return currState; //if we are rolled back, then we were rolled back
        TxnView parentTxnView = getParentTxnView();
        if(Txn.ROOT_TRANSACTION.equals(parentTxnView)) return currState;
        else return parentTxnView.getEffectiveState();
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
           * We know that the otherTxn is effectively active, but we don't
           * necessarily know where in the chain we are considered active. As
           * a result, we need to look at these transactions at the common level.
           *
           * To do this, we find the lowest active transaction in otherTxn's chain(called LAT),
           * and the transaction immediately below it (called below).
           *
           * If the LAT is an ancestor of this transaction, then use the commit timestamp from below.
           *
           * If the LAT is a descendant of this transaction, then we must modify our visibility rules
           * as follows: If the isolation level is SNAPSHOT_ISOLATION or READ_COMMITTED, use
           * the READ_COMMITTED semantics. If the level is READ_UNCOMMITTED, use READ_UNCOMMITTED semantics.
           */

          TxnView t = otherTxn;
          TxnView below = null;
          while(!t.equals(Txn.ROOT_TRANSACTION) && t.getState()!=Txn.State.ACTIVE){
              if(t.getState()== Txn.State.ROLLEDBACK) return false; //never see rolled back transactions
              below = t;
              t = t.getParentTxnView();
          }

          if(equals(t) || t.descendsFrom(this)){
              //we are an ancestor, so use READ_COMMITTED/READ_UNCOMMITTED semantics
              Txn.IsolationLevel level = isolationLevel;
              if(level== Txn.IsolationLevel.SNAPSHOT_ISOLATION)
                  level = Txn.IsolationLevel.READ_COMMITTED;

              return level.canSee(beginTimestamp,otherTxn,true);
          }
          else if(descendsFrom(t)){
              //we are a descendant of the LAT. Thus, we use the commit timestamp of below
              if(below==null){
                  //we are a child of otherTxn, so we can see the reads
                  return true;
              }

              return isolationLevel.canSee(beginTimestamp,below,false);
          }else{
             /*
              * We are not on the same transaction chain, so just use normal isolation level semantics.
              * Note that in most cases, this will return false
              */
              return isolationLevel.canSee(beginTimestamp,otherTxn,false);
          }

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
        switch(otherTxn.getEffectiveState()){
            case ROLLEDBACK: return ConflictType.NONE; //cannot conflict with ourself
            case COMMITTED:
                if(otherTxn.descendsFrom(this)) return ConflictType.CHILD;
                /*
                 * If otherTxn is committed, then we cannot be an ancestor (by definition,
                 * we are assuming that we are active when this method is called). Therefore,
                 * we can check the conflict directly
                 */
                return otherTxn.getEffectiveCommitTimestamp()>beginTimestamp? ConflictType.SIBLING: ConflictType.NONE;
        }


        /*
         * We know that otherTxn is effectively active, but we don't necessarily know
         * where in the chain we are considered active. Therefore, we must navigate the tree
         * to find the lowest active transaction, and the transaction immediately below it.
         *
         * If the lowest active transaction is a descendant of ours, then this is a child conflict.
         *
         * If the lowest active transaction is an ancestor of ours, then we have the common ancestor. In this
         * case, we compare the commit timestamp of the transaction immediately BELOW the lowest active transaction
         * to our begin timestamp to determine whether or not it is visible.
         *
         */
        TxnView t = otherTxn;
        TxnView below = null;
        while(!t.equals(Txn.ROOT_TRANSACTION) && t.getState()!= Txn.State.ACTIVE){
            //we don't need to check roll backs because getEffectiveState() would have been rolled back in that case
            below = t;
            t = t.getParentTxnView();
        }

        if(t.descendsFrom(this)) return ConflictType.CHILD;
        else if(this.descendsFrom(t)){
            // this is the common ancestor that we care about
            if(below==null){
                //we are a child of otherTxn, so no conflict
                return ConflictType.NONE;
            }
            return below.getCommitTimestamp()>beginTimestamp? ConflictType.SIBLING: ConflictType.NONE;
        }else{
            //we do not descend from t, and t does not descend from us. We are unrelated, and still active.
            //therefore, we conflict
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
