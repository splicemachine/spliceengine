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

package com.splicemachine.si.impl;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 8/19/14
 */
public abstract class BaseTransaction implements Transaction {
    private static Logger LOG=Logger.getLogger(BaseTransaction.class);
    protected String transName;

    protected volatile int state;

    public static final int CLOSED=0;
    public static final int IDLE=1;
    public static final int ACTIVE=2;

    public void setTransactionName(String s){
        this.transName=s;
    }

    public String getTransactionName(){
        return this.transName;
    }

    public void commitNoSync(int commitflag) throws IOException {
        SpliceLogUtils.debug(LOG,"commitNoSync commitflag"+commitflag);
        commit();
    }

    public boolean isClosed() {
        return state == CLOSED;
    }

    public void close() {
        SpliceLogUtils.debug(LOG,"close");

        clearState();
        state=CLOSED;
    }

    public abstract boolean allowsWrites();

    protected abstract void clearState();

    public void destroy() throws IOException {
        SpliceLogUtils.debug(LOG,"destroy");
        if(state!=CLOSED)
            abort();
        close();
    }

    @Override
    public int setSavePoint(String name,Object kindOfSavepoint) throws IOException {
        return 0;
    }

    @Override
    public int releaseSavePoint(String name,Object kindOfSavepoint) throws IOException{
        return 0;
    }

    @Override
    public int rollbackToSavePoint(String name,Object kindOfSavepoint) throws IOException{
        return 0;
    }

    @Override
    public boolean anyoneBlocked(){
        return false;
    }

    @Override
    public void createXATransactionFromLocalTransaction(int format_id,byte[] global_id,byte[] branch_id) throws IOException{
    }

    @Override
    public int xa_prepare() throws IOException{
        return 0;
    }

    @Override
    public boolean isIdle(){
        return (state==IDLE);
    }

    @Override
    public boolean isPristine(){
        return (state==IDLE || state==ACTIVE);
    }

    @Override
    public void xa_rollback() throws IOException{
        abort();
    }

    @Override
    public void setNoLockWait(boolean noWait){
    }

    @Override
    public void xa_commit(boolean onePhase) throws IOException{
        SpliceLogUtils.debug(LOG,"xa_commit");
        try{
            if(onePhase)
                commit();
            else{
                xa_prepare();
                commit();
            }
        }catch(Exception e){
            throw new IOException(e);
        }
    }

    public abstract TxnView getTxnInformation();

    public abstract void setActiveState(boolean nested,boolean additive,TxnView parentTxn,byte[] table);

    public abstract void setActiveState(boolean nested,boolean additive,TxnView parentTxn);

    public TxnView getActiveStateTxn(){
        setActiveState(false,false,null);
        return getTxnInformation();
    }

    public abstract void setTxn(Txn txn);
}
