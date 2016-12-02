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

package com.splicemachine.si.impl.store;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.si.impl.ForwardingTxnView;
import com.splicemachine.si.impl.txn.WritableTxn;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.ByteSlice;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.primitives.Longs;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple Transaction store useful for Unit testing. NOT TO BE USED OUTSIDE OF TESTING. This is not thread safe!
 *
 * @author Scott Fines
 *         Date: 6/23/14
 */
public class TestingTxnStore implements TxnStore{
    private final Map<Long, TxnHolder> txnMap;
    private final TimestampSource commitTsGenerator;
    private final Clock clock;
    private TxnLifecycleManager tc;
    private final long txnTimeOutIntervalMs;
    private final ExceptionFactory exceptionFactory;


    public TestingTxnStore(Clock clock,
                           TimestampSource commitTsGenerator,
                           ExceptionFactory exceptionFactory,
                           long txnTimeOutIntervalMs){
        this.txnMap=new ConcurrentHashMap<>();
        this.commitTsGenerator=commitTsGenerator;
        this.txnTimeOutIntervalMs=txnTimeOutIntervalMs;
        this.exceptionFactory=exceptionFactory;
        this.clock=clock;
    }

    @Override
    public Txn getTransaction(long txnId) throws IOException{
        TxnHolder txn=txnMap.get(txnId);
        if(txn==null) return null;

        if(isTimedOut(txn))
            return getRolledbackTxn(txn.txn);
        else return txn.txn;
    }

    @Override
    public Txn getTransaction(long txnId,boolean getDestinationTables) throws IOException{
        return getTransaction(txnId);
    }


    @Override
    public boolean transactionCached(long txnId){
        return false;
    }

    @Override
    public void cache(TxnView toCache){

    }

    @Override
    public Txn getTransactionFromCache(long txnId){
        try{
            return getTransaction(txnId);
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void recordNewTransaction(Txn txn) throws IOException{
        TxnHolder txn1=txnMap.get(txn.getTxnId());
        assert txn1==null:" Transaction "+txn.getTxnId()+" already existed!";
        txnMap.put(txn.getTxnId(),new TxnHolder(txn,clock.currentTimeMillis()));
    }

    @Override
    public void rollback(long txnId) throws IOException{
        TxnHolder txnHolder=txnMap.get(txnId);
        if(txnHolder==null) return; //no transaction exists

        Txn txn=txnHolder.txn;

        Txn.State state=txn.getState();
        if(state!=Txn.State.ACTIVE) return; //nothing to do if we aren't active
        txnHolder.txn=getRolledbackTxn(txn);
    }

    @Override
    public void rollbackSubtransactions(long txnId, LongOpenHashSet subtransactions) throws IOException {
        // TODO
    }

    private Txn getRolledbackTxn(final Txn txn){
        return new ForwardingTxnView(txn){
            @Override
            public void commit() throws IOException{
                throw new UnsupportedOperationException("Txn is rolled back");
            }

            @Override
            public void rollback() throws IOException{
            }

            @Override
            public Txn elevateToWritable(byte[] writeTable) throws IOException{
                throw new UnsupportedOperationException("Txn is rolled back");
            }

            @Override
            public Txn.State getState(){
                return Txn.State.ROLLEDBACK;
            }
        };
    }

    @Override
    public long commit(long txnId) throws IOException{
        TxnHolder txnHolder=txnMap.get(txnId);
        if(txnHolder==null) throw new IOException("Cannot commit txn "+txnId);

        final Txn txn=txnHolder.txn;
        if(txn.getEffectiveState()==Txn.State.ROLLEDBACK)
            throw new IOException("Cannot commit txn "+txnId+", state is "+Txn.State.ROLLEDBACK);
        if(isTimedOut(txnHolder))
            throw new IOException("Cannot commit txn "+txnId+", state is "+Txn.State.ROLLEDBACK);

        final long commitTs=commitTsGenerator.nextTimestamp();

        TxnView parentTransaction=txn.getParentTxnView();
        final long globalCommitTs;
        if(parentTransaction==null || parentTransaction.equals(Txn.ROOT_TRANSACTION))
            globalCommitTs=commitTs;
        else{
            //see if the parent has committed yet
            if(parentTransaction.getEffectiveState()==Txn.State.COMMITTED){
                globalCommitTs=parentTransaction.getEffectiveCommitTimestamp();
            }else
                globalCommitTs=-1l;
        }
        txnHolder.txn=new ForwardingTxnView(txn){
            @Override
            public void commit() throws IOException{
            } //do nothing

            @Override
            public void rollback() throws IOException{
                throw new UnsupportedOperationException("Cannot rollback a committed transaction");
            }

            @Override
            public Txn elevateToWritable(byte[] writeTable) throws IOException{
                throw new UnsupportedOperationException("Txn is committed");
            }

            @Override
            public long getCommitTimestamp(){
                return commitTs;
            }

            @Override
            public long getGlobalCommitTimestamp(){
                return globalCommitTs;
            }

            @Override
            public Txn.State getState(){
                return Txn.State.COMMITTED;
            }
        };
        return commitTs;
    }

    @Override
    public boolean keepAlive(long txnId) throws IOException{
        TxnHolder holder=txnMap.get(txnId);
        if(holder==null) return false;

        Txn txn=holder.txn;
        if(txn.getState()!=Txn.State.ACTIVE)
            return false; //don't keep keepAlives going if the transaction is finished
        if(isTimedOut(holder))
            throw new IOException("Txn "+txnId+" is rolled back");

        holder.keepAliveTs=clock.currentTimeMillis();
        return true;
    }

    //		@Override
    public void timeout(long txnId) throws IOException{
        rollback(txnId);
    }

    @Override
    public void elevateTransaction(Txn txn,byte[] newDestinationTable) throws IOException{
        long txnId=txn.getTxnId();
        Txn writableTxnCopy=new WritableTxn(txn,tc,newDestinationTable,exceptionFactory);
        TxnHolder oldTxn=txnMap.get(txnId);
        if(oldTxn==null){
            txnMap.put(txnId,new TxnHolder(writableTxnCopy,clock.currentTimeMillis()));
        }else{
            assert oldTxn.txn.getEffectiveState()==Txn.State.ACTIVE:"Cannot elevate transaction "+txnId+" because it is not active";
            oldTxn.txn=writableTxnCopy;
        }
    }

    @Override
    public long[] getActiveTransactionIds(Txn txn,byte[] table) throws IOException{
        long minTs=this.commitTsGenerator.retrieveTimestamp();
        return getActiveTransactionIds(minTs,txn.getTxnId(),table);
    }

    @Override
    public long[] getActiveTransactionIds(long minTxnId,long maxTxnId,byte[] table) throws IOException{
        if(table==null)
            return getAllActiveTransactions(minTxnId,maxTxnId);
        else
            return findActiveTransactions(minTxnId,maxTxnId,table);
    }

    @Override
    public List<TxnView> getActiveTransactions(long minTxnid,long maxTxnId,byte[] table) throws IOException{
        List<TxnView> txns= Lists.newArrayListWithExpectedSize(txnMap.size());
        for(Map.Entry<Long, TxnHolder> txnEntry : txnMap.entrySet()){
            if(isTimedOut(txnEntry.getValue())) continue;
            Txn value=txnEntry.getValue().txn;
            if(value.getEffectiveState()==Txn.State.ACTIVE
                    && value.getTxnId()<=maxTxnId
                    && value.getTxnId()>=minTxnid)
                txns.add(value);
        }
        Collections.sort(txns,new Comparator<TxnView>(){
            @Override
            public int compare(TxnView o1,TxnView o2){
                if(o1==null){
                    if(o2==null) return 0;
                    return -1;
                }else if(o2==null) return 1;
                return Longs.compare(o1.getTxnId(), o2.getTxnId());
            }
        });
        return txns;
    }

    @Override
    public long lookupCount(){
        return 0;
    }

    @Override
    public long elevationCount(){
        return 0;
    }

    @Override
    public long createdCount(){
        return 0;
    }

    @Override
    public long rollbackCount(){
        return 0;
    }

    @Override
    public long commitCount(){
        return 0;
    }

    public boolean keepAlive(Txn txn) throws IOException{
        TxnHolder txnHolder=txnMap.get(txn.getTxnId());
        if(txnHolder==null) return false; //nothing to keep alive
        if(txnHolder.txn.getEffectiveState()==Txn.State.ACTIVE && isTimedOut(txnHolder))
            throw new IOException("Txn "+txn.getTxnId()+" is rolled back");
        if(txn.getEffectiveState()!=Txn.State.ACTIVE) return false;

        txnHolder.keepAliveTs=clock.currentTimeMillis();
        return true;
    }

    private boolean isTimedOut(TxnHolder txn){
        return txn.txn.getEffectiveState()==Txn.State.ACTIVE &&
                (clock.currentTimeMillis()-txn.keepAliveTs)>txnTimeOutIntervalMs;
    }

    private long[] getAllActiveTransactions(long minTimestamp,long maxId) throws IOException{

        LongArrayList activeTxns=new LongArrayList(txnMap.size());
        for(Map.Entry<Long, TxnHolder> txnEntry : txnMap.entrySet()){
            if(isTimedOut(txnEntry.getValue())) continue;
            Txn value=txnEntry.getValue().txn;
            if(value.getEffectiveState()==Txn.State.ACTIVE
                    && value.getTxnId()<=maxId
                    && value.getTxnId()>=minTimestamp)
                activeTxns.add(txnEntry.getKey());
        }
        return activeTxns.toArray();
    }

    private long[] findActiveTransactions(long minTimestamp,long maxId,byte[] table){
        LongArrayList activeTxns=new LongArrayList(txnMap.size());
        for(Map.Entry<Long, TxnHolder> txnEntry : txnMap.entrySet()){
            if(isTimedOut(txnEntry.getValue())) continue;
            Txn value=txnEntry.getValue().txn;
            if(value.getEffectiveState()==Txn.State.ACTIVE && value.getTxnId()<=maxId && value.getTxnId()>=minTimestamp){
                Iterator<ByteSlice> destinationTables=value.getDestinationTables();
                while(destinationTables.hasNext()){
                    ByteSlice data=destinationTables.next();
                    if(data.equals(table,0,table.length))
                        activeTxns.add(txnEntry.getKey());
                }
            }
        }
        return activeTxns.toArray();
    }

    public void setLifecycleManager(TxnLifecycleManager lifecycleManager){
        this.tc=lifecycleManager;
    }

    private static class TxnHolder{
        private volatile Txn txn;
        private volatile long keepAliveTs;

        private TxnHolder(Txn txn,long keepAliveTs){
            this.txn=txn;
            this.keepAliveTs=keepAliveTs;
        }

        @Override
        public String toString(){
            return "TxnHolder{"+
                    "txn="+txn+
                    ", keepAliveTs="+keepAliveTs+
                    '}';
        }
    }

    @Override
    public void setCache(TxnSupplier cache){
        // no op
    }
}
