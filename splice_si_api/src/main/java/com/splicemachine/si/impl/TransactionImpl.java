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

    private Deque<TransactionState> txnStack=new LinkedList<>();

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
        txnStack.push(new TransactionState(transName,txn));
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

        setActiveState(false,false,null); //make sure that we are active
        TransactionState currentTxn=txnStack.peek();
        while(!currentTxn.txn.allowsSubtransactions() && currentTxn.released) {
            // We have some transactions that were released and are no longer useful (they don't allow subtransactions)
            // so we finish processing them and pop them from the stack
            currentTxn.txn.commit();
            txnStack.pop();
            currentTxn=txnStack.peek();
        }
        // if the current transaction doesn't allow subtransactions the child transaction we create here is going to be
        // a persisted transaction, we want to keep it around after we release the savepoint to be able to create subtransactions
        // out of it
        boolean keep = !currentTxn.txn.allowsSubtransactions();
        Txn child=lifecycleManager.beginChildTransaction(currentTxn.txn,null);
        txnStack.push(new TransactionState(name,child,keep));
        return txnStack.size();
    }

    @Override
    public int releaseSavePoint(String name,Object kindOfSavepoint) throws IOException{
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
        for(TransactionState savePoint : txnStack){
            if(savePoint.name.equals(name)){
                found=true;
                break;
            }
        }
        if(!found)
            throw new SavePointNotFoundException(name);

        /*
         * Pop all the transactions up until the last that allows subtransactions
         * We might leave transactions in the stack (1 for every 256 created) but we allow
         * the creation of subtransactions in memory
         */
        TransactionState savePoint;
        boolean keep = false;
        Iterator<TransactionState> it = txnStack.iterator();
        do {
            savePoint = it.next();
            keep |= savePoint.keep;
            // if we are to keep this transaction (or some txn earlier in the stack) we mark it as released but defer
            // committing it and removing it from the stack until later
            if (keep) {
                savePoint.released = true;
            } else {
                doCommit(savePoint);
                it.remove();
            }
        } while(!savePoint.name.equals(name));
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"After releaseSavePoint: name=%s, savePointStack=\n%s",name,getSavePointStackString());
        return txnStack.size();
    }

    public String getActiveStateTxnName(){
        return txnStack.peek().name;
    }

    private void doCommit(TransactionState savePoint) throws IOException{
        savePoint.txn.commit();
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
        for(TransactionState savePoint : txnStack){
            if(savePoint.name.equals(name)){
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
        TransactionState savePoint;
        do{
            savePoint=txnStack.pop();
            savePoint.txn.rollback(); //rollback the child transaction
        }while(!savePoint.name.equals(name));

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
                SpliceLogUtils.trace(LOG,"The transaction is in idle state and there is nothing to commit, transID="+(txnStack.peekLast()==null?"null":txnStack.getLast().txn));
            return;
        }
        if(state==CLOSED){
            throw new IOException("Transaction has already closed and cannot commit again");
        }
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"commit, state="+state+" for transaction "+(txnStack.peekLast()==null?"null":txnStack.getLast().txn));

        while(!txnStack.isEmpty()){
            TransactionState userPair=txnStack.pop();
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
        while(!txnStack.isEmpty()){
            txnStack.pop().txn.rollback();
        }
        state=IDLE;
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"After abort: state=%s, savePointStack=\n%s",getTransactionStatusAsString(),getSavePointStackString());
    }

    public String getActiveStateTxIdString(){
        SpliceLogUtils.debug(LOG,"getActiveStateTxIdString");
        setActiveState(false,false,null);
        if(!txnStack.isEmpty())
            return txnStack.peek().txn.toString();
        else
            return null;
    }

    public Txn getActiveStateTxn(){
        setActiveState(false,false,null);
        if(!txnStack.isEmpty())
            return txnStack.peek().txn;
        else
            return null;
    }

    public void setActiveState(boolean nested,boolean additive,TxnView parentTxn){
        setActiveState(nested,additive,parentTxn,null);
    }

    public void setActiveState(boolean nested,boolean additive,TxnView parentTxn,byte[] table){
        if(state==IDLE){
            try{
                synchronized(this){
                    SpliceLogUtils.trace(LOG,"setActiveState: parent "+parentTxn);
                    Txn txn;
                    if(nested)
                        txn=lifecycleManager.beginChildTransaction(parentTxn,parentTxn.getIsolationLevel(),additive,table);
                    else
                        txn=lifecycleManager.beginTransaction();

                    txnStack.push(new TransactionState(transName,txn));
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
        if(!txnStack.isEmpty())
            return txnStack.peek().txn;
        return null;
    }

    public void setTxn(Txn txn){
        this.txnStack.peek().txn = txn;
    }

    public Txn elevate(byte[] writeTable) throws IOException{
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"Before elevate: state=%s, savePointStack=\n%s",getTransactionStatusAsString(),getSavePointStackString());
        /*
         * We want to elevate the transaction. HOWEVER, we need to ensure that the entire
         * stack has been elevated first.
         */
        setActiveState(false,false,null);
        Iterator<TransactionState> parents=txnStack.descendingIterator();
        Txn lastTxn=null;
        while(parents.hasNext()){
            TransactionState next=parents.next();
            Txn n=doElevate(writeTable,next.txn,lastTxn);
            next.txn = n;
            lastTxn=n;
        }
        if(LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"After elevate: state=%s, savePointStack=\n%s",getTransactionStatusAsString(),getSavePointStackString());
        return txnStack.peek().txn;
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
        for(TransactionState savePoint : txnStack){
            sb.append(String.format("name=%s, txn=%s%n",savePoint.name,savePoint.txn));
        }
        return sb.toString();
    }

    public boolean allowsWrites(){
        Txn txn=getTxn();
        return txn!=null && txn.allowsWrites();
    }

    @Override
    public boolean isRestoreMode() {
        return lifecycleManager != null && lifecycleManager.isRestoreMode();
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

    private static class TransactionState {
        /** Savepoint name */
        String name;
        /** Transaction associated with it */
        Txn txn;
        /** Have we already released this savepoint, even though it's still int the stack? */
        boolean released;
        /** Whether to keep this transaction in the stack instead of committing and removing it from the stack when the savepoint
         * is released
         */
        boolean keep;

        public TransactionState(String name, Txn txn, boolean keep) {
            this(name, txn);
            this.keep = keep;
        }

        public TransactionState(String name, Txn txn) {
            this.name = name;
            this.txn = txn;
            this.released = false;
        }

        @Override
        public String toString() {
            return "TransactionState{" +
                    "name='" + name + '\'' +
                    ", txn=" + txn +
                    ", released=" + released +
                    ", keep=" + keep +
                    '}';
        }
    }
}
