package com.splicemachine.si.impl;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SliceIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * Builder object for constructing a TransactionView.
 *
 * @author Scott Fines
 * Date: 8/14/14
 */
public class TxnViewBuilder {

    private long txnId = -1l;
    private long beginTimestamp = -1l;
    private long commitTimestamp = -1l;
    private long globalCommitTimestamp = -1l;
    private Txn.State currentState;
    private Txn.IsolationLevel isolationLevel;

    private boolean hasDependent;
    private boolean dependent;
    private boolean hasAdditive;
    private boolean additive;
    private boolean hasAllowWrites;
    private boolean allowWrites;

    private Collection<byte[]> destinationTables = null;

    private long lastKeepAliveTime = -1l;

    private TxnSupplier txnSupplier;
    private long parentTxnId = -1l;

    public TxnViewBuilder parentTxnId(long parentTxnId){
        this.parentTxnId = parentTxnId;
        return this;
    }

    public TxnViewBuilder txnId(long txnId){
        this.txnId = txnId;
        return this;
    }

    public TxnViewBuilder beginTimestamp(long beginTimestamp){
        this.beginTimestamp = beginTimestamp;
        return this;
    }

    public TxnViewBuilder commitTimestamp(long commitTimestamp){
        this.commitTimestamp = commitTimestamp;
        return this;
    }

    public TxnViewBuilder globalCommitTimestamp(long globalCommitTimestamp){
        this.globalCommitTimestamp = globalCommitTimestamp;
        return this;
    }

    public TxnViewBuilder state(Txn.State state){
        this.currentState = state;
        return this;
    }

    public TxnViewBuilder dependent(boolean dependent){
        this.hasDependent = true;
        this.dependent = dependent;
        return this;
    }

    public TxnViewBuilder allowWrites(boolean allowWrites){
        this.hasAllowWrites = true;
        this.allowWrites = allowWrites;
        return this;
    }

    public TxnViewBuilder additive(boolean additive){
        this.hasAdditive = true;
        this.additive = additive;
        return this;
    }

    public TxnViewBuilder isolationLevel(Txn.IsolationLevel level){
        this.isolationLevel = level;
        return this;
    }

    public TxnViewBuilder keepAliveTimestamp(long keepAliveTimestamp){
        this.lastKeepAliveTime = keepAliveTimestamp;
        return this;
    }

    public TxnViewBuilder destinationTable(byte[] destinationTable){
        if(destinationTables==null)
            destinationTables = Lists.newArrayList();
        destinationTables.add(destinationTable);
        return this;
    }

    public TxnViewBuilder store(TxnSupplier txnSupplier){
        this.txnSupplier = txnSupplier;
        return this;
    }

    public TxnView build() throws IOException {
        assert txnId>=0: "No transaction id specified";
        assert beginTimestamp>=0: "No begin timestamp specified";
        assert currentState!=null: "No Current state specified";

        TxnView parentTxn;
        if(parentTxnId<0)
            parentTxn = Txn.ROOT_TRANSACTION;
        else{
            assert txnSupplier!=null: "No Transaction store supplied";
            parentTxn = txnSupplier.getTransaction(parentTxnId);
        }

        Iterator<ByteSlice> destTableIterator;
        if(destinationTables==null)
            destTableIterator = Iterators.emptyIterator();
        else
            destTableIterator= new SliceIterator(destinationTables.iterator());
        return new InheritingTxnView(parentTxn,
                txnId,beginTimestamp,isolationLevel,hasDependent,dependent,
                hasAdditive,additive,hasAllowWrites,allowWrites,
                commitTimestamp,globalCommitTimestamp,currentState,
                destTableIterator,lastKeepAliveTime);
    }
}
