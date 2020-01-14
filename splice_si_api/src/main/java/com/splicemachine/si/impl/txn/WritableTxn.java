/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.TaskId;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SliceIterator;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Represents a Transaction.
 *
 * @author Scott Fines
 *         Date: 6/18/14
 */
public class WritableTxn extends AbstractTxn{
    private static final Logger LOG=Logger.getLogger(WritableTxn.class);
    private volatile TxnView parentTxn;
    private volatile long commitTimestamp=-1l;
    private volatile long globalCommitTimestamp=-1l;
    private TxnLifecycleManager tc;
    private boolean isAdditive;
    private volatile State state=State.ACTIVE;
    private Set<byte[]> tableWrites=new CopyOnWriteArraySet<>();
    private TaskId taskId;
    private ExceptionFactory exceptionFactory;

    public WritableTxn(){

    }

    public WritableTxn(long txnId,
                       long beginTimestamp,
                       Txn parentRoot,
                       IsolationLevel isolationLevel,
                       TxnView parentTxn,
                       TxnLifecycleManager tc,
                       boolean isAdditive,
                       ExceptionFactory exceptionFactory){
        this(txnId,beginTimestamp,parentRoot,isolationLevel,parentTxn,tc,isAdditive,null,null,exceptionFactory);
    }

    public WritableTxn(long txnId,
                       long beginTimestamp,
                       Txn parentReference,
                       IsolationLevel isolationLevel,
                       TxnView parentTxn,
                       TxnLifecycleManager tc,
                       boolean isAdditive,
                       byte[] destinationTable,
                       TaskId taskId,
                       ExceptionFactory exceptionFactory){
        super(parentReference,txnId,beginTimestamp,isolationLevel);
        this.exceptionFactory = exceptionFactory;
        this.parentTxn=parentTxn;
        this.tc=tc;
        this.isAdditive=isAdditive;
        this.taskId=taskId;

        if(destinationTable!=null)
            this.tableWrites.add(destinationTable);
    }

    public WritableTxn(Txn txn,TxnLifecycleManager tc,byte[] destinationTable,ExceptionFactory exceptionFactory){
        super(txn, txn.getTxnId(), txn.getBeginTimestamp(),txn.getIsolationLevel());
        this.parentTxn=txn.getParentTxnView();
        this.tc=tc;
        this.isAdditive=txn.isAdditive();
        if(destinationTable!=null)
            this.tableWrites.add(destinationTable);
        this.exceptionFactory = exceptionFactory;
    }

    @Override
    public boolean isAdditive(){
        return isAdditive;
    }

    @Override
    public long getGlobalCommitTimestamp(){
        if(globalCommitTimestamp<0) return parentTxn.getGlobalCommitTimestamp();
        return globalCommitTimestamp;
    }

    @Override
    public long getCommitTimestamp(){
        return commitTimestamp;
    }

    @Override
    public long getEffectiveCommitTimestamp(){
        if(state==State.ROLLEDBACK) return -1l;
        if(globalCommitTimestamp>=0) return globalCommitTimestamp;
        if(Txn.ROOT_TRANSACTION.equals(parentTxn))
            globalCommitTimestamp=commitTimestamp;
        else
            globalCommitTimestamp=parentTxn.getEffectiveCommitTimestamp();

        return globalCommitTimestamp;
    }

    @Override
    public TxnView getParentTxnView(){
        return parentTxn;
    }

    @Override
    public State getState(){
        return state;
    }

    @Override
    public void commit() throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"Before commit: txn=%s",this);

        if (getSubId() == 0) {
            switch (state) {
                case COMMITTED:
                    return;
                case ROLLEDBACK:
                    throw exceptionFactory.cannotCommit(txnId, state);
            }
            synchronized (this) {
                //double-checked locking for efficiency--usually not needed
                switch (state) {
                    case COMMITTED:
                        return;
                    case ROLLEDBACK:
                        throw exceptionFactory.cannotCommit(txnId, state);
                    case ACTIVE:
                        if (ROOT_TRANSACTION.equals(parentTxn)) {
                            tc.unregisterActiveTransaction(getBeginTimestamp());
                        }
                        commitTimestamp = tc.commit(txnId);
                        state = State.COMMITTED;
                }
            }
        } else {
            state = State.COMMITTED;
        }
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"After commit: txn=%s,commitTimestamp=%s",this,commitTimestamp);
    }

    @Override
    public void subRollback() {
        for(Txn c : children) {
            c.subRollback();
        }
        parentReference.addRolledback(getSubId());
        state = State.ROLLEDBACK;
    }

    @Override
    public void rollback() throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"Before rollback: txn=%s",this);

        if (getSubId() == 0) {
            switch (state) {
                case COMMITTED://don't need to rollback
                case ROLLEDBACK:
                    return;
            }

            synchronized (this) {
                switch (state) {
                    case COMMITTED://don't need to rollback
                    case ROLLEDBACK:
                        return;
                }

                if (ROOT_TRANSACTION.equals(parentTxn)) {
                    tc.unregisterActiveTransaction(getBeginTimestamp());
                }
                tc.rollback(txnId);
                state = State.ROLLEDBACK;
            }
        } else {
            subRollback();
            tc.rollbackSubtransactions(txnId, parentReference.getRolledback());
        }
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"After rollback: txn=%s",this);
    }

    @Override
    public boolean allowsWrites(){
        return true;
    }

    @Override
    public Txn elevateToWritable(byte[] writeTable) throws IOException{
        assert state==State.ACTIVE:"Cannot elevate an inactive transaction: "+state;
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
    public Iterator<ByteSlice> getDestinationTables(){
        return new SliceIterator(tableWrites.iterator());
    }

    public WritableTxn getReadUncommittedActiveTxn() {
        return new WritableTxn(txnId,getBeginTimestamp(), parentReference,Txn.IsolationLevel.READ_UNCOMMITTED,
                parentTxn,tc,isAdditive,null,null,exceptionFactory);
    }

    public WritableTxn getReadCommittedActiveTxn() {
        return new WritableTxn(txnId,getBeginTimestamp(), parentReference,Txn.IsolationLevel.READ_COMMITTED,
                parentTxn,tc,isAdditive,null,null,exceptionFactory);
    }

    @Override
    public TaskId getTaskId() {
        return taskId;
    }
}
