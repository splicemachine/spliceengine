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

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.locks.CompatibilitySpace;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.api.txn.TxnRegistry;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

public class SpliceTransaction extends BaseSpliceTransaction{
    public static final String BATCH_SAVEPOINT="BATCH_SAVEPOINT";
    private static Logger LOG=Logger.getLogger(SpliceTransaction.class);
    private boolean ignoreSavePoints;

    private Deque<Pair<String, Txn>> txnStack=new LinkedList<>();
    private final TxnRegistry registry;

    public SpliceTransaction(CompatibilitySpace compatibilitySpace,
                             SpliceTransactionFactory spliceTransactionFactory,
                             DataValueFactory dataValueFactory,
                             String transName,TxnRegistry registry){
        this.registry=registry;
        SpliceLogUtils.trace(LOG,"Instantiating Splice transaction");
        this.compatibilitySpace=compatibilitySpace;
        this.spliceTransactionFactory=spliceTransactionFactory;
        this.dataValueFactory=dataValueFactory;
        this.transName=transName;
        this.state=IDLE;
        this.ignoreSavePoints=SIDriver.driver().getConfiguration().ignoreSavePoints();
    }

    public SpliceTransaction(CompatibilitySpace compatibilitySpace,
                             SpliceTransactionFactory spliceTransactionFactory,
                             DataValueFactory dataValueFactory,
                             String transName,
                             Txn txn ){
        this.registry=null;
        SpliceLogUtils.trace(LOG,"Instantiating Splice transaction");
        this.compatibilitySpace=compatibilitySpace;
        this.spliceTransactionFactory=spliceTransactionFactory;
        this.dataValueFactory=dataValueFactory;
        this.transName=transName;
        this.state=ACTIVE;
        txnStack.push(Pair.newPair(transName,txn));
        this.ignoreSavePoints=EngineDriver.driver().getConfiguration().ignoreSavePoints();
    }

    @Override
    public int setSavePoint(String name,Object kindOfSavepoint) throws StandardException{
        if(ignoreSavePoints)
            return 1;
        if(kindOfSavepoint!=null && BATCH_SAVEPOINT.equals(kindOfSavepoint)){
            ignoreSavePoints=true;
        }

        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Before setSavePoint: name=%s, savePointStack=\n%s",name,getSavePointStackString());
        setActiveState(false,false,null); //make sure that we are active
        Txn currentTxn=getTxn();
        try{
            Txn child=SIDriver.driver().lifecycleManager().beginChildTransaction(currentTxn,null);
            txnStack.push(Pair.newPair(name,child));
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"After setSavePoint: name=%s, savePointStack=\n%s",name,getSavePointStackString());
        return txnStack.size();
    }

    @Override
    public int releaseSavePoint(String name,Object kindOfSavepoint) throws StandardException{
        if(kindOfSavepoint!=null && BATCH_SAVEPOINT.equals(kindOfSavepoint)){
            ignoreSavePoints=false;
        }
        if(ignoreSavePoints)
            return 0;
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Before releaseSavePoint: name=%s, savePointStack=\n%s",name,getSavePointStackString());


        /*
         * Check first to ensure such a save point exists before we attempt to release anything
         */
        boolean found=false;
        for(Pair<String, Txn> savePoint : txnStack){
            if(savePoint.getFirst().equals(name)){
                found=true;
                break;
            }
        }
        if(!found)
            throw ErrorState.XACT_SAVEPOINT_NOT_FOUND.newException(name);

        /*
         * Pop all the transactions up until the savepoint to release.
         *
         * Note that we do *NOT* commit all the transactions that are higher
         * on the stack than the savepoint we wish to release. This is because
         * we are committing a parent of that transaction,and committing the parent
         * will treat the underlying transaction as committed also. This saves on excessive
         * network calls when releasing multiple save points, at the cost of looking a bit weird
         * here.
         */
        Pair<String, Txn> savePoint;
        do{
            savePoint=txnStack.pop();
            doCommit(savePoint);
        }while(!savePoint.getFirst().equals(name));
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"After releaseSavePoint: name=%s, savePointStack=\n%s",name,getSavePointStackString());
        return txnStack.size();
    }

    public String getActiveStateTxnName(){
        return txnStack.peek().getFirst();
    }

    private void doCommit(Pair<String, Txn> savePoint) throws StandardException{
        try{
            savePoint.getSecond().commit();
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public int rollbackToSavePoint(String name,Object kindOfSavepoint) throws StandardException{
        if(ignoreSavePoints)
            return 0;
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Before rollbackToSavePoint: name=%s, savePointStack=\n%s",name,getSavePointStackString());
        /*
         * Check first to ensure such a save point exists before we attempt to release anything
         */
        boolean found=false;
        for(Pair<String, Txn> savePoint : txnStack){
            if(savePoint.getFirst().equals(name)){
                found=true;
                break;
            }
        }
        if(!found)
            throw ErrorState.XACT_SAVEPOINT_NOT_FOUND.newException(name);

        /*
         * Pop all the transactions up until the savepoint to rollback.
         *
         * Note that we do not have to rollback each child in this transaction, because
         * we are rolling back a parent, and that would in turn cause the children to be
         * rolled back as well. However, we do this to improve efficiency of reads during this
         * case--rows written with these savePoints will be immediately seen as rolled back, and
         * no navigation will be required.
         */
        Pair<String, Txn> savePoint;
        do{
            savePoint=txnStack.pop();
            try{
                savePoint.getSecond().rollback(); //commit the child transaction
            }catch(IOException e){
                throw Exceptions.parseException(e);
            }
        }while(!savePoint.getFirst().equals(name));

        /*
         * In effect, we've removed the save point (because we've rolled it back). Thus,
         * we need to set up a new savepoint context so that future writes are observed within
         * the proper savepoint context.
         */
        int numSavePoints=setSavePoint(name,kindOfSavepoint);
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"After rollbackToSavePoint: name=%s, savePointStack=\n%s",name,getSavePointStackString());
        return numSavePoints;
    }

    @Override
    public void commit() throws StandardException{
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Before commit: state=%s, savePointStack=\n%s",getTransactionStatusAsString(),getSavePointStackString());
        if(state==IDLE){
            if(LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG,"The transaction is in idle state and there is nothing to commit, transID="+(txnStack.peekLast()==null?"null":txnStack.getLast().getSecond()));
            return;
        }
        if(state==CLOSED){
            throw StandardException.newException("Transaction has already closed and cannot commit again");
        }
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"commit, state="+state+" for transaction "+(txnStack.peekLast()==null?"null":txnStack.getLast().getSecond()));

        Txn lastTxn = null;
        while(txnStack.size()>0){
            Pair<String, Txn> userPair=txnStack.pop();
            doCommit(userPair);
            lastTxn = userPair.getSecond();
        }
        if(lastTxn!=null && registry!=null)
            registry.deregisterTxn(lastTxn);

        //throw away all savepoints
        txnStack.clear();
        state=IDLE;
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"After commit: state=%s, savePointStack=\n%s",getTransactionStatusAsString(),getSavePointStackString());
        return;
    }


    public void abort() throws StandardException{
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Before abort: state=%s, savePointStack=\n%s",getTransactionStatusAsString(),getSavePointStackString());
        SpliceLogUtils.debug(LOG,"abort");
        try{
            if(state!=ACTIVE)
                return;
            Txn lastTxn = null;
            while(txnStack.size()>0){
                Txn second=txnStack.pop().getSecond();
                second.rollback();
                lastTxn = second;
            }
            if(lastTxn!=null && registry!=null)
                registry.deregisterTxn(lastTxn);

            state=IDLE;
        }catch(Exception e){
            throw StandardException.newException(e.getMessage(),e);
        }
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"After abort: state=%s, savePointStack=\n%s",getTransactionStatusAsString(),getSavePointStackString());
    }

    public String getActiveStateTxIdString(){
        SpliceLogUtils.debug(LOG,"getActiveStateTxIdString");
        setActiveState(false,false,null);
        if(txnStack.size()>0)
            return txnStack.peek().getSecond().toString();
        else
            return null;
    }

    public Txn getActiveStateTxn(){
        setActiveState(false,false,null);
        if(txnStack.size()>0)
            return txnStack.peek().getSecond();
        else
            return null;
    }

    public final String getContextId(){
        SpliceTransactionContext tempxc=transContext;
        return (tempxc==null)?null:tempxc.getIdName();
    }

    public final void setActiveState(boolean nested,boolean additive,TxnView parentTxn){
        setActiveState(nested,additive,parentTxn,null);
    }

    public final void setActiveState(boolean nested,boolean additive,TxnView parentTxn,byte[] table){
        if(state==IDLE){
            try{
                synchronized(this){
                    SpliceLogUtils.trace(LOG,"setActiveState: parent "+parentTxn);
                    TxnLifecycleManager lifecycleManager=SIDriver.driver().lifecycleManager();
                    Txn txn;
                    if(nested)
                        txn=lifecycleManager.beginChildTransaction(parentTxn,parentTxn.getIsolationLevel(),additive,table);
                    else{
                        txn=lifecycleManager.beginTransaction();
                        assert registry!=null: "Programmer error: creating top-level transaction without a registry!";
                        registry.registerTxn(txn);
                    }

                    txnStack.push(Pair.newPair(transName,txn));
                    state=ACTIVE;
                }
            }catch(Exception e){
                SpliceLogUtils.logAndThrowRuntime(LOG,e);
            }
        }
    }

    public int getTransactionStatus(){
        return state;
    }

    public Txn getTxn(){
        if(txnStack.size()>0)
            return txnStack.peek().getSecond();
        return null;
    }

    public void setTxn(Txn txn){
        this.txnStack.peek().setSecond(txn);
    }

    public Txn elevate(byte[] writeTable) throws StandardException{
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Before elevate: state=%s, savePointStack=\n%s",getTransactionStatusAsString(),getSavePointStackString());
        /*
         * We want to elevate the transaction. HOWEVER, we need to ensure that the entire
         * stack has been elevated first.
         */
        setActiveState(false,false,null);
        Iterator<Pair<String, Txn>> parents=txnStack.descendingIterator();
        Txn lastTxn=null;
        while(parents.hasNext()){
            Pair<String, Txn> next=parents.next();
            Txn n=doElevate(writeTable,next.getSecond(),lastTxn);
            next.setSecond(n);
            lastTxn=n;
        }
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"After elevate: state=%s, savePointStack=\n%s",getTransactionStatusAsString(),getSavePointStackString());
        return txnStack.peek().getSecond();
    }

    @Override
    protected void clearState(){
        txnStack.clear();
    }

    @Override
    public TxnView getTxnInformation(){
        return getTxn();
    }

    @Override
    public String toString(){
        return "SpliceTransaction["+getTransactionStatusAsString()+","+getTxn()+"]";
    }

    /**
     * Return the state of the transaction as a string (e.g. IDLE, ACTIVE, CLOSED, etc.).
     *
     * @return the current state of the transaction
     */
    private String getTransactionStatusAsString(){
        if(state==IDLE)
            return "IDLE";
        else if(state==ACTIVE)
            return "ACTIVE";
        else
            return "CLOSED";
    }

    /**
     * Return a string depicting the savepoints stack with the current savepoint being at the top and its predecessors below it.
     *
     * @return string depicting the savepoints stack
     */
    private String getSavePointStackString(){
        StringBuilder sb=new StringBuilder();
        for(Pair<String, Txn> savePoint : txnStack){
            sb.append(String.format("name=%s, txn=%s%n",savePoint.getFirst(),savePoint.getSecond()));
        }
        return sb.toString();
    }

    public boolean allowsWrites(){
        Txn txn=getTxn();
        return txn!=null && txn.allowsWrites();
    }

    /*****************************************************************************************************************/
    /*private helper methods*/
    private Txn doElevate(byte[] writeTable,Txn currentTxn,TxnView elevatedParent) throws StandardException{
        if(!currentTxn.allowsWrites()){
            if(elevatedParent!=null){
                assert currentTxn instanceof ReadOnlyTxn:"Programmer error: current transaction is not a ReadOnlyTxn";
                ((ReadOnlyTxn)currentTxn).parentWritable(elevatedParent);
            }
            try{
                currentTxn=currentTxn.elevateToWritable(writeTable);
            }catch(IOException e){
                throw Exceptions.parseException(e);
            }
        }
        return currentTxn;
    }
}
