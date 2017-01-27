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

package com.splicemachine.derby.impl.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.daemon.Serviceable;
import com.splicemachine.db.iapi.services.locks.CompatibilitySpace;
import com.splicemachine.db.iapi.services.property.PersistentSet;
import com.splicemachine.db.iapi.store.access.FileResource;
import com.splicemachine.db.iapi.store.raw.*;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 8/19/14
 */
public abstract class BaseSpliceTransaction<T extends BaseTransaction> implements Transaction{
    private static Logger LOG=Logger.getLogger(BaseSpliceTransaction.class);
    CompatibilitySpace compatibilitySpace;
    SpliceTransactionFactory spliceTransactionFactory;
    DataValueFactory dataValueFactory;
    SpliceTransactionContext transContext;
    protected T transaction;

    void setTransactionName(String s){
        transaction.setTransactionName(s);
    }

    String getTransactionName(){
        return transaction.getTransactionName();
    }

    public void commitNoSync(int commitFlag) throws StandardException{
        SpliceLogUtils.debug(LOG,"commitNoSync commitFlag"+commitFlag);
        try {
            transaction.commitNoSync(commitFlag);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    public void close() throws StandardException{
        SpliceLogUtils.debug(LOG,"close");

        if(transContext!=null){
            transContext.popMe();
            transContext=null;
        }
        transaction.close();
    }

    public abstract boolean allowsWrites();

    public void destroy() throws StandardException{
        SpliceLogUtils.debug(LOG,"destroy");
        if (!transaction.isClosed())
            try {
                transaction.abort();
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
        close();
    }

    @Override
    public DataValueFactory getDataValueFactory() throws StandardException{
        return dataValueFactory;
    }

    @Override
    public int setSavePoint(String name,Object kindOfSavepoint) throws StandardException{
        try {
            return transaction.setSavePoint(name, kindOfSavepoint);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public int releaseSavePoint(String name,Object kindOfSavepoint) throws StandardException{
        try {
            return transaction.releaseSavePoint(name, kindOfSavepoint);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public int rollbackToSavePoint(String name,Object kindOfSavepoint) throws StandardException{
        try {
            return transaction.rollbackToSavePoint(name, kindOfSavepoint);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void addPostCommitWork(Serviceable work){
    }

    @Override
    public void addPostTerminationWork(Serviceable work){
    }

    @Override
    public FileResource getFileHandler(){
        return (spliceTransactionFactory==null?null:spliceTransactionFactory.getFileHandler());
    }

    @Override
    public boolean anyoneBlocked(){
        return false;
    }

    @Override
    public void createXATransactionFromLocalTransaction(int format_id,byte[] global_id,byte[] branch_id) throws StandardException{
    }

    @Override
    public int xa_prepare() throws StandardException{
        return 0;
    }

    @Override
    public boolean isIdle(){
        return transaction.isIdle();
    }

    @Override
    public boolean isPristine(){
        return transaction.isPristine();
    }

    @Override
    public void xa_rollback() throws StandardException{
        abort();
    }

    @Override
    public ContextManager getContextManager(){
        return transContext.getContextManager();
    }

    @Override
    public CompatibilitySpace getCompatibilitySpace(){
        return compatibilitySpace;
    }

    @Override
    public void setNoLockWait(boolean noWait){
    }

    @Override
    public void setup(PersistentSet set) throws StandardException{
    }

    @Override
    public GlobalTransactionId getGlobalId(){
        return null;
    }

    @Override
    public void xa_commit(boolean onePhase) throws StandardException{
        SpliceLogUtils.debug(LOG,"xa_commit");
        try{
            transaction.xa_commit(onePhase);
        }catch(Exception e){
            throw StandardException.newException(e.getMessage(),e);
        }
    }

    public abstract TxnView getTxnInformation();

    public abstract void setActiveState(boolean nested,boolean additive,TxnView parentTxn);

    public TxnView getActiveStateTxn(){
        return transaction.getActiveStateTxn();
    }
}
