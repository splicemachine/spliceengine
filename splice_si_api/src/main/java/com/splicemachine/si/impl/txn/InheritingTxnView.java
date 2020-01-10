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

import com.splicemachine.si.api.txn.TaskId;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.ByteSlice;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Iterator;

/**
 * Transaction which is partially constructed, but which looks up values in order
 * to complete itself when necessary.
 * <p/>
 * Useful primarily for child transactions during initial construction, in order
 * to populate default values.
 *
 * @author Scott Fines
 *         Date: 6/19/14
 */
public class InheritingTxnView extends AbstractTxnView{
    private final boolean hasAdditive;
    private final boolean isAdditive;
    private final TxnView parentTxn;
    private final long commitTimestamp;
    private final Txn.State state;
    private final boolean allowWrites;
    private final boolean hasAllowWrites;
    private long globalCommitTimestamp;
    private final Iterator<ByteSlice> destinationTables;
    private final long lastKaTime;
    private final TaskId taskId;

    @SuppressFBWarnings("SE_NO_SUITABLE_CONSTRUCTOR_FOR_EXTERNALIZATION")
    public InheritingTxnView(TxnView parentTxn,
                             long txnId,long beginTimestamp,
                             boolean allowWrites,
                             Txn.IsolationLevel isolationLevel,
                             Txn.State state){
        this(parentTxn,
                txnId,
                beginTimestamp,
                isolationLevel,
                false,false,
                true,allowWrites,-1l,-1l,state);
    }

    public InheritingTxnView(TxnView parentTxn,
                             long txnId,long beginTimestamp,
                             Txn.IsolationLevel isolationLevel,
                             Txn.State state){
        this(parentTxn,
                txnId,
                beginTimestamp,
                isolationLevel,
                false,false,false,false,-1l,-1l,state);
    }

    public InheritingTxnView(TxnView parentTxn,
                             long txnId,long beginTimestamp,
                             Txn.IsolationLevel isolationLevel,
                             boolean hasAdditive,boolean isAdditive,
                             boolean hasAllowWrites,boolean allowWrites,
                             long commitTimestamp,long globalCommitTimestamp,
                             Txn.State state){
        this(parentTxn,txnId,beginTimestamp,isolationLevel,
                hasAdditive,isAdditive,
                hasAllowWrites,allowWrites,
                commitTimestamp,globalCommitTimestamp,
                state,Collections.<ByteSlice>emptyIterator());
    }

    public InheritingTxnView(TxnView parentTxn,
                             long txnId,long beginTimestamp,
                             Txn.IsolationLevel isolationLevel,
                             boolean hasAdditive,boolean isAdditive,
                             boolean hasAllowWrites,boolean allowWrites,
                             long commitTimestamp,long globalCommitTimestamp,
                             Txn.State state,
                             Iterator<ByteSlice> destinationTables){
        this(parentTxn,txnId,beginTimestamp,isolationLevel,
                hasAdditive,isAdditive,
                hasAllowWrites,allowWrites,
                commitTimestamp,globalCommitTimestamp,
                state,destinationTables,-1l,null);
    }

    public InheritingTxnView(TxnView parentTxn,
                             long txnId,long beginTimestamp,
                             Txn.IsolationLevel isolationLevel,
                             boolean hasAdditive,boolean isAdditive,
                             boolean hasAllowWrites,boolean allowWrites,
                             long commitTimestamp,long globalCommitTimestamp,
                             Txn.State state,
                             Iterator<ByteSlice> destinationTables,
                             long lastKaTime,TaskId taskId){
        super(txnId,beginTimestamp,isolationLevel);
        this.hasAdditive=hasAdditive;
        this.isAdditive=isAdditive;
        this.parentTxn=parentTxn;
        this.commitTimestamp=commitTimestamp;
        this.state=state;
        this.allowWrites=allowWrites;
        this.hasAllowWrites=hasAllowWrites;
        this.globalCommitTimestamp=globalCommitTimestamp;
        this.destinationTables=destinationTables;
        this.lastKaTime=lastKaTime;
        this.taskId=taskId;
    }

    @Override
    public long getLastKeepAliveTimestamp(){
        return lastKaTime;
    }

    @Override
    public Iterator<ByteSlice> getDestinationTables(){
        return destinationTables;
    }

    @Override
    public boolean isAdditive(){
        if(hasAdditive) return isAdditive;
        return parentTxn.isAdditive();
    }

    @Override
    public long getGlobalCommitTimestamp(){
        if(state==Txn.State.ROLLEDBACK) return -1l; //can't have a global commit timestamp if we are rolled back
        if(globalCommitTimestamp==-1l) return parentTxn.getGlobalCommitTimestamp();
        return globalCommitTimestamp;
    }

    @Override
    public Txn.IsolationLevel getIsolationLevel(){
        if(isolationLevel!=null) return isolationLevel;
        return parentTxn.getIsolationLevel();
    }

    @Override
    public long getCommitTimestamp(){
        return commitTimestamp;
    }

    @Override
    public TxnView getParentTxnView(){
        return parentTxn;
    }

    @Override
    public Txn.State getState(){
        return state;
    }

    @Override
    public long getEffectiveCommitTimestamp(){
        if(state==Txn.State.ROLLEDBACK) return -1l;

        if(globalCommitTimestamp>=0) return globalCommitTimestamp;
        if(Txn.ROOT_TRANSACTION.equals(parentTxn)){
            globalCommitTimestamp=commitTimestamp;
        }else{
            globalCommitTimestamp=parentTxn.getEffectiveCommitTimestamp();
        }
        return globalCommitTimestamp;
    }

    @Override
    public boolean allowsWrites(){
        if(hasAllowWrites) return allowWrites;
        return parentTxn.allowsWrites();
    }

    @Override
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException{
        throw new UnsupportedOperationException("InheritingTxnView is not intended to be serialized");
    }

    @Override
    public void writeExternal(ObjectOutput output) throws IOException{
        throw new UnsupportedOperationException("InheritingTxnView is not intended to be serialized");
    }

    @Override
    public TaskId getTaskId() {
        return taskId;
    }
}
