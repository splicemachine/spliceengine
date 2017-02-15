/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.impl.txn;

import com.splicemachine.si.api.data.ExceptionFactory;
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
        this(txnId,beginTimestamp,parentRoot,isolationLevel,parentTxn,tc,isAdditive,null,exceptionFactory);
    }

    public WritableTxn(long txnId,
                       long beginTimestamp,
                       Txn parentReference,
                       IsolationLevel isolationLevel,
                       TxnView parentTxn,
                       TxnLifecycleManager tc,
                       boolean isAdditive,
                       byte[] destinationTable,
                       ExceptionFactory exceptionFactory){
        super(parentReference,txnId,beginTimestamp,isolationLevel);
        this.exceptionFactory = exceptionFactory;
        this.parentTxn=parentTxn;
        this.tc=tc;
        this.isAdditive=isAdditive;

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
                parentTxn,tc,isAdditive,null,exceptionFactory);
    }

    public WritableTxn getReadCommittedActiveTxn() {
        return new WritableTxn(txnId,getBeginTimestamp(), parentReference,Txn.IsolationLevel.READ_COMMITTED,
                parentTxn,tc,isAdditive,null,exceptionFactory);
    }


}