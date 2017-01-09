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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.locks.CompatibilitySpace;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.impl.TransactionImpl;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.io.IOException;

public class SpliceTransaction extends BaseSpliceTransaction<TransactionImpl> {
    private static Logger LOG=Logger.getLogger(SpliceTransaction.class);

    public SpliceTransaction(CompatibilitySpace compatibilitySpace,
                             SpliceTransactionFactory spliceTransactionFactory,
                             DataValueFactory dataValueFactory,
                             String transName){
        SpliceLogUtils.trace(LOG,"Instantiating Splice transaction");
        this.compatibilitySpace=compatibilitySpace;
        this.spliceTransactionFactory=spliceTransactionFactory;
        this.dataValueFactory=dataValueFactory;
        boolean ignoreSavepoints = SIDriver.driver().getConfiguration().ignoreSavePoints();
        TxnLifecycleManager lifecycleManager = SIDriver.driver().lifecycleManager();
        this.transaction = new TransactionImpl(transName, ignoreSavepoints, lifecycleManager);
    }

    public SpliceTransaction(CompatibilitySpace compatibilitySpace,
                             SpliceTransactionFactory spliceTransactionFactory,
                             DataValueFactory dataValueFactory,
                             String transName,Txn txn){
        SpliceLogUtils.trace(LOG,"Instantiating Splice transaction");
        this.compatibilitySpace=compatibilitySpace;
        this.spliceTransactionFactory=spliceTransactionFactory;
        this.dataValueFactory=dataValueFactory;
        boolean ignoreSavepoints = SIDriver.driver().getConfiguration().ignoreSavePoints();
        TxnLifecycleManager lifecycleManager = SIDriver.driver().lifecycleManager();
        this.transaction = new TransactionImpl(transName, txn, ignoreSavepoints, lifecycleManager);
    }

    @Override
    public void commit() throws StandardException{
        try {
            transaction.commit();
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }


    public void abort() throws StandardException{
        SpliceLogUtils.debug(LOG,"abort");
        try{
            transaction.abort();
        }catch(Exception e){
            throw StandardException.newException(e.getMessage(),e);
        }
    }

    public String getActiveStateTxIdString(){
        SpliceLogUtils.debug(LOG,"getActiveStateTxIdString");
        return transaction.getActiveStateTxIdString();
    }

    public Txn getActiveStateTxn(){
        return transaction.getActiveStateTxn();
    }

    public final String getContextId(){
        SpliceTransactionContext tempxc=transContext;
        return (tempxc==null)?null:tempxc.getIdName();
    }

    public final void setActiveState(boolean nested,boolean additive,Txn parentTxn){
        transaction.setActiveState(nested,additive,parentTxn,null);
    }

    public final void setActiveState(boolean nested,boolean additive,Txn parentTxn,byte[] table){
        transaction.setActiveState(nested, additive, parentTxn, table);
    }

    public int getTransactionStatus(){
        return transaction.getTransactionStatus();
    }

    public Txn getTxn(){
        return transaction.getTxn();
    }

    public void setTxn(Txn txn){
        transaction.setTxn(txn);
    }

    public Txn elevate(byte[] writeTable) throws StandardException{
        try {
            return transaction.elevate(writeTable);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }


    @Override
    public Txn getTxnInformation(){
        return getTxn();
    }

    @Override
    public String toString(){
        return "SpliceTransaction["+transaction.getTransactionStatusAsString()+","+getTxn()+"]";
    }

    public boolean allowsWrites(){
        return transaction.allowsWrites();
    }
}
