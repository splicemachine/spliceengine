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

package com.splicemachine.si.impl;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

public class TransactionImpl extends BaseTransaction {
    public static final String BATCH_SAVEPOINT="BATCH_SAVEPOINT";
    private static Logger LOG=Logger.getLogger(TransactionImpl.class);
    private boolean ignoreSavePoints;
    private TxnLifecycleManager lifecycleManager;

    private Deque<Pair<String, Txn>> txnStack=new LinkedList<>();

    public TransactionImpl(String transName, boolean ignoreSavePoints, TxnLifecycleManager lifecycleManager){
        SpliceLogUtils.trace(LOG,"Instantiating Splice transaction");
        this.transName=transName;
        this.state=IDLE;
        this.ignoreSavePoints=ignoreSavePoints;
        this.lifecycleManager = lifecycleManager;
    }

    public TransactionImpl(String transName, Txn txn, boolean ignoreSavePoints, TxnLifecycleManager lifecycleManager){
        SpliceLogUtils.trace(LOG,"Instantiating Splice transaction");
        this.transName=transName;
        this.state=ACTIVE;
        txnStack.push(Pair.newPair(transName,txn));
        this.ignoreSavePoints=ignoreSavePoints;
        this.lifecycleManager = lifecycleManager;
    }

    @Override
    public int setSavePoint(String name,Object kindOfSavepoint) throws IOException{
        if(ignoreSavePoints)
            return 1;
        if(kindOfSavepoint!=null && BATCH_SAVEPOINT.equals(kindOfSavepoint)){
            ignoreSavePoints=true;
        }

        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Before setSavePoint: name=%s, savePointStack=\n%s",name,getSavePointStackString());
        setActiveState(false,false,null); //make sure that we are active
        Txn currentTxn=getTxn();
        Txn child=lifecycleManager.beginChildTransaction(currentTxn,null);
        txnStack.push(Pair.newPair(name,child));
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"After setSavePoint: name=%s, savePointStack=\n%s",name,getSavePointStackString());
        return txnStack.size();
    }

    @Override
    public int releaseSavePoint(String name,Object kindOfSavepoint) throws IOException{
        // Don't do anything, we'll release the savepoints when we commit the parent transaction
        return txnStack.size();
    }

    public String getActiveStateTxnName(){
        return txnStack.peek().getFirst();
    }

    private void doCommit(Pair<String, Txn> savePoint) throws IOException{
        savePoint.getSecond().commit();
    }

    @Override
    public int rollbackToSavePoint(String name,Object kindOfSavepoint) throws IOException{
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
            throw new SavePointNotFoundException(name);

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
            savePoint.getSecond().rollback(); //rollback the child transaction
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
    public void commit() throws IOException{
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Before commit: state=%s, savePointStack=\n%s",getTransactionStatusAsString(),getSavePointStackString());
        if(state==IDLE){
            if(LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG,"The transaction is in idle state and there is nothing to commit, transID="+(txnStack.peekLast()==null?"null":txnStack.getLast().getSecond()));
            return;
        }
        if(state==CLOSED){
            throw new IOException("Transaction has already closed and cannot commit again");
        }
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"commit, state="+state+" for transaction "+(txnStack.peekLast()==null?"null":txnStack.getLast().getSecond()));

        while(txnStack.size()>0){
            Pair<String, Txn> userPair=txnStack.pop();
            doCommit(userPair);
        }

        //throw away all savepoints
        txnStack.clear();
        state=IDLE;
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"After commit: state=%s, savePointStack=\n%s",getTransactionStatusAsString(),getSavePointStackString());
        return;
    }


    public void abort() throws IOException{
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Before abort: state=%s, savePointStack=\n%s",getTransactionStatusAsString(),getSavePointStackString());
        SpliceLogUtils.debug(LOG,"abort");
        if(state!=ACTIVE)
            return;
        while(txnStack.size()>0){
            txnStack.pop().getSecond().rollback();
        }
        state=IDLE;
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

    public final void setActiveState(boolean nested,boolean additive,TxnView parentTxn){
        setActiveState(nested,additive,parentTxn,null);
    }

    public final void setActiveState(boolean nested,boolean additive,TxnView parentTxn,byte[] table){
        if(state==IDLE){
            try{
                synchronized(this){
                    SpliceLogUtils.trace(LOG,"setActiveState: parent "+parentTxn);
                    Txn txn;
                    if(nested)
                        txn=lifecycleManager.beginChildTransaction(parentTxn,parentTxn.getIsolationLevel(),additive,table);
                    else
                        txn=lifecycleManager.beginTransaction();

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

    public Txn elevate(byte[] writeTable) throws IOException{
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
    public String getTransactionStatusAsString(){
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
    private Txn doElevate(byte[] writeTable,Txn currentTxn,TxnView elevatedParent) throws IOException{
        if(!currentTxn.allowsWrites()){
            if(elevatedParent!=null){
                assert currentTxn instanceof ReadOnlyTxn:"Programmer error: current transaction is not a ReadOnlyTxn";
                ((ReadOnlyTxn)currentTxn).parentWritable(elevatedParent);
            }
            currentTxn=currentTxn.elevateToWritable(writeTable);
        }
        return currentTxn;
    }
}
