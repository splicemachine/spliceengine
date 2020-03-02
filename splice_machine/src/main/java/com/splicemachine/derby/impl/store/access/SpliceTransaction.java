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

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.locks.CompatibilitySpace;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnView;
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
        this(compatibilitySpace, spliceTransactionFactory, dataValueFactory,
                new TransactionImpl(transName,
                        SIDriver.driver().getConfiguration().ignoreSavePoints(),
                        SIDriver.driver().lifecycleManager()));
    }

    public SpliceTransaction(CompatibilitySpace compatibilitySpace,
                             SpliceTransactionFactory spliceTransactionFactory,
                             DataValueFactory dataValueFactory,
                             String transName,Txn txn){
        this(compatibilitySpace, spliceTransactionFactory, dataValueFactory,
                new TransactionImpl(transName, txn,
                        SIDriver.driver().getConfiguration().ignoreSavePoints(),
                        SIDriver.driver().lifecycleManager()));
    }

    protected SpliceTransaction(CompatibilitySpace compatibilitySpace,
                             SpliceTransactionFactory spliceTransactionFactory,
                             DataValueFactory dataValueFactory,
                             TransactionImpl txn){
        SpliceLogUtils.trace(LOG,"Instantiating Splice transaction");
        this.compatibilitySpace=compatibilitySpace;
        this.spliceTransactionFactory=spliceTransactionFactory;
        this.dataValueFactory=dataValueFactory;
        this.transaction = txn;
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

    public final void setActiveState(boolean nested,boolean additive,TxnView parentTxn){
        transaction.setActiveState(nested,additive,parentTxn,null);
    }

    public final void setActiveState(boolean nested,boolean additive,TxnView parentTxn,byte[] table){
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
    public TxnView getTxnInformation(){
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
