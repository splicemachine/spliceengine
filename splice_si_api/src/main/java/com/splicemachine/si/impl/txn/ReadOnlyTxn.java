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
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
@SuppressFBWarnings("SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION")
public class ReadOnlyTxn extends AbstractTxn{

    private static final Logger LOG=Logger.getLogger(ReadOnlyTxn.class);
    private volatile TxnView parentTxn;
    private AtomicReference<State> state=new AtomicReference<State>(State.ACTIVE);
    private final TxnLifecycleManager tc;
    private final boolean additive;
    private final ExceptionFactory exceptionFactory;
    private final TaskId taskId;

    public static Txn create(long txnId,IsolationLevel isolationLevel,TxnLifecycleManager tc,ExceptionFactory exceptionFactory){
        return new ReadOnlyTxn(txnId,txnId,isolationLevel,Txn.ROOT_TRANSACTION,tc,exceptionFactory,false);
    }

    public static Txn wrapReadOnlyInformation(TxnView myInformation,TxnLifecycleManager control,ExceptionFactory exceptionFactory){
        return new ReadOnlyTxn(myInformation.getTxnId(),
                myInformation.getBeginTimestamp(),
                myInformation.getIsolationLevel(),
                myInformation.getParentTxnView(),
                control,
                exceptionFactory,
                myInformation.isAdditive());
    }

    public static Txn createReadOnlyTransaction(long txnId,
                                                TxnView parentTxn,
                                                long beginTs,
                                                IsolationLevel level,
                                                boolean additive,
                                                TxnLifecycleManager control,
                                                ExceptionFactory exceptionFactory){
        return new ReadOnlyTxn(txnId,beginTs,level,parentTxn,control,exceptionFactory,additive);
    }

    public static ReadOnlyTxn createReadOnlyChildTransaction(
            TxnView parentTxn,
            TxnLifecycleManager tc,
            boolean additive,
            ExceptionFactory exceptionFactory){
        //make yourself a copy of the parent transaction, for the purposes of reading
        return new ReadOnlyTxn(parentTxn.getTxnId(),
                        parentTxn.getBeginTimestamp(),
                        parentTxn.getIsolationLevel(),parentTxn,tc,exceptionFactory,additive);
    }

    public static ReadOnlyTxn createReadOnlyParentTransaction(long txnId,long beginTimestamp,
                                                              IsolationLevel isolationLevel,
                                                              TxnLifecycleManager tc,
                                                              ExceptionFactory exceptionFactory,
                                                              boolean additive){
        return new ReadOnlyTxn(txnId,beginTimestamp,isolationLevel,ROOT_TRANSACTION,tc,exceptionFactory,additive);
    }

    public ReadOnlyTxn(long txnId,
                       long beginTimestamp,
                       IsolationLevel isolationLevel,
                       TxnView parentTxn,
                       TxnLifecycleManager tc,
                       ExceptionFactory exceptionFactory,
                       boolean additive){
        this(txnId,beginTimestamp, isolationLevel, parentTxn, tc,
                exceptionFactory,
                additive, null);
    }

    public ReadOnlyTxn(long txnId,
                       long beginTimestamp,
                       IsolationLevel isolationLevel,
                       TxnView parentTxn,
                       TxnLifecycleManager tc,
                       ExceptionFactory exceptionFactory,
                       boolean additive,
                       TaskId taskId){
        super(null, txnId,beginTimestamp,isolationLevel);
        this.parentTxn=parentTxn;
        this.tc=tc;
        this.additive=additive;
        this.exceptionFactory = exceptionFactory;
        this.taskId = taskId;
    }

    @Override
    public boolean isAdditive(){
        return additive;
    }

    @Override
    public long getCommitTimestamp(){
        return -1l; //read-only transactions do not need to commit
    }

    @Override
    public long getGlobalCommitTimestamp(){
        return -1l; //read-only transactions do not need a global commit timestamp
    }

    @Override
    public long getEffectiveCommitTimestamp(){
        if(state.get()==State.ROLLEDBACK) return -1l;
        if(parentTxn!=null)
            return parentTxn.getEffectiveCommitTimestamp();
        return -1l; //read-only transactions do not need to commit, so they don't need a TxnId
    }

    @Override
    public TxnView getParentTxnView(){
        return parentTxn;
    }

    @Override
    public State getState(){
        return state.get();
    }

    @Override
    public void subRollback() {
        for (Txn c : children) {
            c.subRollback();
        }

        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"Before rollback: txn=%s",this);
        boolean shouldContinue;
        do{
            State currState=state.get();
            switch(currState){
                case COMMITTED:
                case ROLLEDBACK:
                    return;
                default:
                    shouldContinue=state.compareAndSet(currState,State.ROLLEDBACK);
            }
        }while(shouldContinue);
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"After rollback: txn=%s",this);
    }

    @Override
    public void commit() throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"Before commit: txn=%s",this);
        boolean shouldContinue;
        do{
            State currState=state.get();
            switch(currState){
                case COMMITTED:
                    return;
                case ROLLEDBACK:
                    throw exceptionFactory.cannotCommit(txnId,currState);
                default:
                    shouldContinue=!state.compareAndSet(currState,State.COMMITTED);
            }
        }while(shouldContinue);

        if (ROOT_TRANSACTION.equals(parentTxn)) {
            tc.unregisterActiveTransaction(getBeginTimestamp());
        }
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"After commit: txn=%s",this);
    }

    @Override
    public void rollback() throws IOException{
        subRollback();

        if (ROOT_TRANSACTION.equals(parentTxn)) {
            tc.unregisterActiveTransaction(getBeginTimestamp());
        }
    }

    @Override
    public boolean allowsWrites(){
        return false;
    }

    @Override
    public Txn elevateToWritable(byte[] writeTable) throws IOException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"Before elevateToWritable: txn=%s,writeTable=%s",this,writeTable);
        assert state.get()==State.ACTIVE:"Cannot elevate an inactive transaction!";
        Txn newTxn;
        if((parentTxn!=null && !ROOT_TRANSACTION.equals(parentTxn))){
           /*
		    * We are a read-only child transaction of a parent. This means that we didn't actually
			* create a child transaction id or a begin timestamp of our own. Instead of elevating,
			* we actually create a writable child transaction.
			*/
            newTxn=tc.beginChildTransaction(parentTxn,isolationLevel,additive,writeTable,true);
        }else{
            newTxn=tc.elevateTransaction(this,writeTable); //requires at least one network call
        }
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"After elevateToWritable: newTxn=%s",newTxn);
        return newTxn;
    }

    public void parentWritable(TxnView newParentTxn){
        if(newParentTxn==parentTxn) return;
        this.parentTxn=newParentTxn;
    }
}
