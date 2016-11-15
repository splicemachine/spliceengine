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

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Date: 7/3/14
 */
public class ActiveWriteTxn extends AbstractTxnView{
    private TxnView parentTxn;
    private boolean additive;

    public ActiveWriteTxn(){
        super();
    }

    public ActiveWriteTxn(long txnId,
                          long beginTimestamp,
                          TxnView parentTxn,
                          boolean additive,
                          Txn.IsolationLevel isolationLevel){
        super(txnId,beginTimestamp,isolationLevel);
        this.parentTxn=parentTxn;
        this.additive=additive;
    }


    @Override
    public long getCommitTimestamp(){
        return -1l;
    }

    @Override
    public long getEffectiveCommitTimestamp(){
        return -1l;
    }

    @Override
    public long getGlobalCommitTimestamp(){
        return -1l;
    }

    @Override
    public TxnView getParentTxnView(){
        return parentTxn;
    }

    @Override
    public long getParentTxnId(){
        return parentTxn.getParentTxnId();
    }

    @Override
    public Txn.State getState(){
        return Txn.State.ACTIVE;
    }

    @Override
    public boolean allowsWrites(){
        return true;
    }

    @Override
    public boolean isAdditive(){
        return additive;
    }

    public void setAdditive(boolean additive) {
        this.additive = additive;
    }
    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException{
        super.readExternal(input);
        additive=input.readBoolean();
        parentTxn=(TxnView)input.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException{
        super.writeExternal(output);
        output.writeBoolean(additive);
        output.writeObject(parentTxn);
    }

    public ActiveWriteTxn getReadUncommittedActiveTxn() {
        return new ActiveWriteTxn(txnId,getBeginTimestamp(),parentTxn,additive, Txn.IsolationLevel.READ_UNCOMMITTED);
    }

    public ActiveWriteTxn getReadCommittedActiveTxn() {
        return new ActiveWriteTxn(txnId,getBeginTimestamp(),parentTxn,additive, Txn.IsolationLevel.READ_COMMITTED);
    }

}