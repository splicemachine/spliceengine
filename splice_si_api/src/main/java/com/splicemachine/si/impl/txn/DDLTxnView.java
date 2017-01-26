/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.txn.ConflictType;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.txn.AbstractTxnView;
import com.splicemachine.utils.ByteSlice;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;

/**
 * A view of a transaction that has slightly modified visibility semantics.
 *
 * In particular, there is a single point in time, called the "demarcation point"; any
 * transaction which begins after this demarcation point is <em>not</em> visible
 * to this transaction (even if it were otherwise visible).
 *
 * Additionally, we operate under the "single-ancestor" assumption--that is, that
 * we should behave as if we are the immediate child of a top-level transaction (even if we are
 * not, which we won't always be). This allows us to create a child transaction to manage the overall
 * DDL operation which acts as if it's a top-level transaction, but is in fact a child.
 *
 * @author Scott Fines
 * Date: 8/27/14
 */
public class DDLTxnView extends AbstractTxnView {
    private TxnView txn;
    private long demarcationPoint;

    @Deprecated//"Serializability constructor: DO NOT USE"
    public DDLTxnView(){ }

    public DDLTxnView(TxnView delegate, long demarcationPoint) {
        super(delegate.getTxnId(),delegate.getBeginTimestamp(),delegate.getIsolationLevel());
        this.txn = delegate;
        this.demarcationPoint = demarcationPoint;
        assert demarcationPoint>=0: "Cannot use a demarcated view with a negative demarcation point";
    }

    @Override public Txn.State getEffectiveState() { return txn.getEffectiveState(); }
    @Override public Txn.IsolationLevel getIsolationLevel() { return txn.getIsolationLevel(); }
    @Override public long getTxnId() { return txn.getTxnId(); }
    @Override public long getBeginTimestamp() { return txn.getBeginTimestamp(); }
    @Override public long getCommitTimestamp() { return txn.getCommitTimestamp(); }
    @Override public long getEffectiveCommitTimestamp() { return txn.getEffectiveCommitTimestamp(); }
    @Override public long getEffectiveBeginTimestamp() { return txn.getEffectiveBeginTimestamp(); }
    @Override public long getLastKeepAliveTimestamp() { return txn.getLastKeepAliveTimestamp(); }
    @Override public TxnView getParentTxnView() { return txn.getParentTxnView(); }
    @Override public long getParentTxnId() { return txn.getParentTxnId(); }
    @Override public Txn.State getState() { return txn.getState(); }
    @Override public boolean allowsWrites() { return txn.allowsWrites(); }
    @Override public boolean isAdditive() { return txn.isAdditive(); }
    @Override public long getGlobalCommitTimestamp() { return txn.getGlobalCommitTimestamp(); }
    @Override public Iterator<ByteSlice> getDestinationTables() { return txn.getDestinationTables(); }
    @Override public boolean descendsFrom(TxnView potentialParent) { return txn.descendsFrom(potentialParent); }

    @Override
    public boolean canSee(TxnView otherTxn) {
        assert otherTxn!=null: "Cannot access visibility semantics of a null transaction!";

        //we cannot see any transaction which begins after our demarcation point
        if(otherTxn.getBeginTimestamp()>=demarcationPoint) return false;

        if(equals(otherTxn)) return true; //you can always see your own writes
        if(isAdditive() && otherTxn.isAdditive()){
            /*
             * Both transactions are additive, but we can only treat them as additive
             * if they are both children of the same parent.
             *
             * However, if they DO have the same parent, then they operate as if they
             * have a READ_UNCOMMITTED isolation level.
             *
             * Note that a readonly child transaction inherits the same transactional
             * structure as the parent (e.g. there is no such thing as a read-only child
             * transaction, you are just reading with the parent transaction). As a result,
             * we say that if otherTxn is a direct child of us, or we are a direct child
             * of otherTxn, then we can also be additive with respect to one another.
             */
            TxnView myParent = getParentTxnView();
            TxnView otherParent = otherTxn.getParentTxnView();
            if(equals(otherParent)
                    || otherTxn.equals(myParent)
                    || !myParent.equals(Txn.ROOT_TRANSACTION) && myParent.equals(otherParent)){
                return Txn.IsolationLevel.READ_UNCOMMITTED.canSee(getBeginTimestamp(),otherTxn,false);
            }
        }
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
        while(t.getState()!=Txn.State.ACTIVE){
            if(t.getState()== Txn.State.ROLLEDBACK) return false; //never see rolled back transactions
            below = t;
            t = t.getParentTxnView();
        }

        if(otherTxn.descendsFrom(this)){
            //we are an ancestor, so use READ_COMMITTED/READ_UNCOMMITTED semantics
            Txn.IsolationLevel level = isolationLevel;
            if(level== Txn.IsolationLevel.SNAPSHOT_ISOLATION)
                level = Txn.IsolationLevel.READ_COMMITTED;

              /*
               * Since we are an ancestor, we use our own begin timestamp to determine the operations.
               */
            return level.canSee(getBeginTimestamp(),otherTxn,true);
        }
        else if(descendsFrom(t)){
            if(below==null) return true; //we are a child of t, so we can see  the reads
              /*
               * We are a descendant of the LAT. Thus, we use the commit timestamp of below,
               * and the begin timestamp of the child of t which is also our ancestor.
               */
            TxnView b = getImmediateChild(t);

            return isolationLevel.canSee(b.getBeginTimestamp(),below,false);
        }else{
             /*
              * We have no transactions in common. One of two things is true:
              *
              * 1. we are at the ROOT transaction => do an isolationLevel visibility on t
              * 2. we are at some node before the transaction => we are active.
              *
              * In either case, we allow the normal transactional semantics to determine our
              * effective state.
              */
            if(Txn.ROOT_TRANSACTION.equals(t))
                t = below; //the next element below

            TxnView b = getParentTxnView();

            return isolationLevel.canSee(b.getBeginTimestamp(),t,false);
        }
    }

    @Override
    public ConflictType conflicts(TxnView otherTxn) {
        return txn.conflicts(otherTxn);
    }

	@Override
	public void readExternal(ObjectInput input) throws IOException,
			ClassNotFoundException {
		super.readExternal(input);
		demarcationPoint = input.readLong();
		txn = (TxnView) input.readObject();
	}

	@Override
	public void writeExternal(ObjectOutput output) throws IOException {
		super.writeExternal(output);
		output.writeLong(demarcationPoint);
		output.writeObject(txn);
	}

    @Override
    public String toString() {
        return "DDL" + txn;
    }
}
