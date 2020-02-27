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
 *
 */

package com.splicemachine.si.impl;

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.txn.PastTxn;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

public class PastTransactionImpl extends TransactionImpl {
    private static Logger LOG=Logger.getLogger(PastTransactionImpl.class);
    Txn txn;

    public PastTransactionImpl(String transName, long transactionId){
        super(transName, false, null);
        SpliceLogUtils.trace(LOG,"Instantiating Splice past transaction");
        this.txn = new PastTxn(transactionId);
    }

    @Override
    public int setSavePoint(String name,Object kindOfSavepoint) throws IOException{
        return 1;
    }

    @Override
    public int releaseSavePoint(String name,Object kindOfSavepoint) throws IOException{
        return 1;
    }

    public String getActiveStateTxnName(){
        return "PAST_TRANSACTION@"+txn.getTxnId();
    }

    @Override
    public int rollbackToSavePoint(String name,Object kindOfSavepoint) throws IOException{
        return 1; // ignore
    }

    @Override
    public void commit() throws IOException{
        // ignore
    }


    public void abort() throws IOException{
        // ignore
    }

    public String getActiveStateTxIdString(){
        return txn.toString();
    }

    public Txn getActiveStateTxn(){
        return txn;
    }

    public final void setActiveState(boolean nested,boolean additive,TxnView parentTxn){
        // ignore
    }

    public final void setActiveState(boolean nested,boolean additive,TxnView parentTxn,byte[] table){
        //ignore
    }


    public void setTxn(Txn txn){
        throw new UnsupportedOperationException("Can't update PastTransaction txn");
    }

    public Txn elevate(byte[] writeTable) throws IOException{
        ExceptionFactory ef = SIDriver.driver().getExceptionFactory();
        throw new IOException(ef.readOnlyModification("Txn "+txn+" is a past transaction, it's read only"));
    }

    @Override
    public Txn getTxn() {
        return txn;
    }

    @Override
    protected void clearState(){
        // ignore
    }

    @Override
    public TxnView getTxnInformation(){
        return txn;
    }

    @Override
    public String toString(){
        return "PastTransaction["+getTransactionStatusAsString()+","+txn+"]";
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


    public boolean allowsWrites(){
        return false;
    }
}
