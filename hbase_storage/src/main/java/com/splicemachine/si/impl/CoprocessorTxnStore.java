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

import com.carrotsearch.hppc.LongHashSet;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.si.api.txn.ActiveTxnTracker;
import com.splicemachine.si.api.txn.TaskId;
import com.splicemachine.si.api.txn.TransactionMissing;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.region.V2TxnDecoder;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import splice.com.google.common.collect.Iterators;
import splice.com.google.common.collect.Lists;
import splice.com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.encoding.DecodingIterator;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.txn.InheritingTxnView;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Transaction Store which uses the TxnLifecycleEndpoint to manage and access transactions
 * remotely.
 * <p/>
 * This class has no local cache. Callers are responsible for caching returned transactions
 * safely.
 *
 * @author Scott Fines
 *         Date: 6/27/14
 */
@ThreadSafe
public class CoprocessorTxnStore implements TxnStore {
    private final TxnNetworkLayerFactory tableFactory;
    private TxnSupplier cache; //a transaction store which uses a global cache for us
    private volatile long oldTransactions;
    private final boolean ignoreMissingTransactions;
    private final ActiveTxnTracker activeTransactions;
    
    @ThreadSafe
    private final TimestampSource timestampSource;

    /*monitoring fields*/
    private final AtomicLong lookups=new AtomicLong(0l);
    private final AtomicLong elevations=new AtomicLong(0l);
    private final AtomicLong txnsCreated=new AtomicLong(0l);
    private final AtomicLong rollbacks=new AtomicLong(0l);
    private final AtomicLong commits=new AtomicLong(0l);

    public CoprocessorTxnStore(TxnNetworkLayerFactory tableFactory,
                               TimestampSource timestampSource,
                               @ThreadSafe TxnSupplier txnCache){
        this.tableFactory=tableFactory;
        this.cache = txnCache==null?this:txnCache; // Not Used...
        this.timestampSource=timestampSource;
        this.ignoreMissingTransactions = HConfiguration.getConfiguration().getIgnoreMissingTxns();
        this.activeTransactions = new ActiveTxnTracker();
    }

    @Override
    public void recordNewTransaction(Txn txn) throws IOException {
        byte[] rowKey=getTransactionRowKey(txn.getTxnId());


        TxnMessage.TxnInfo.Builder request=TxnMessage.TxnInfo.newBuilder()
                .setTxnId(txn.getTxnId())
                .setAllowsWrites(txn.allowsWrites())
                .setIsAdditive(txn.isAdditive())
                .setBeginTs(txn.getBeginTimestamp())
                .setIsolationLevel(txn.getIsolationLevel().encode());

        TaskId taskId = txn.getTaskId();
        if (taskId != null) {
            request.setTaskId(TxnMessage.TaskId.newBuilder()
                    .setPartitionId(taskId.getPartitionId())
                    .setStageId(taskId.getStageId())
                    .setTaskAttemptNumber(taskId.getTaskAttemptNumber())
                    .build());
        }
        
        if(!Txn.ROOT_TRANSACTION.equals(txn.getParentTxnView())){
            request=request.setParentTxnid(txn.getParentTxnId());
        }
        Iterator<ByteSlice> destinationTables=txn.getDestinationTables();
        List<byte[]> bytes=null;
        while(destinationTables.hasNext()){
            if(bytes==null)
                bytes=Lists.newArrayList();
            bytes.add(destinationTables.next().getByteCopy());
        }
        if(bytes!=null){
            MultiFieldEncoder encoder=MultiFieldEncoder.create(bytes.size());
            //noinspection ForLoopReplaceableByForEach
            for(int i=0;i<bytes.size();i++){
                encoder=encoder.encodeNextUnsorted(bytes.get(i));
            }
            ByteString bs=ZeroCopyLiteralByteString.wrap(encoder.build());
            request=request.setDestinationTables(bs);
        }

        try(TxnNetworkLayer table = tableFactory.accessTxnNetwork()){
            table.beginTransaction(rowKey,request.build());
            txnsCreated.incrementAndGet();
        }
    }

    @Override
    public void registerActiveTransaction(Txn txn) {
        activeTransactions.registerActiveTxn(txn.getBeginTimestamp());
    }

    @Override
    public void unregisterActiveTransaction(long txnId) {
        activeTransactions.unregisterActiveTxn(txnId);
    }

    @Override
    public Long oldestActiveTransaction() {
        return activeTransactions.oldestActiveTransaction();
    }

    @Override
    public void rollback(long txnId) throws IOException{
        byte[] rowKey=getTransactionRowKey(txnId);
        TxnMessage.TxnLifecycleMessage lifecycle=TxnMessage.TxnLifecycleMessage.newBuilder()
                .setTxnId(txnId).setAction(TxnMessage.LifecycleAction.ROLLBACk).build();
        try(TxnNetworkLayer table = tableFactory.accessTxnNetwork()){
            table.lifecycleAction(rowKey,lifecycle);
            rollbacks.incrementAndGet();
        }
    }

    @Override
    public void rollbackSubtransactions(long txnId, LongHashSet subtransactions) throws IOException {
        byte[] rowKey=getTransactionRowKey(txnId);
        TxnMessage.TxnLifecycleMessage lifecycle=TxnMessage.TxnLifecycleMessage.newBuilder()
                .setTxnId(txnId).addAllRolledbackSubTxns(Longs.asList(subtransactions.toArray()))
                .setAction(TxnMessage.LifecycleAction.ROLLBACK_SUBTRANSACTIONS).build();
        try(TxnNetworkLayer table = tableFactory.accessTxnNetwork()){
            table.lifecycleAction(rowKey,lifecycle);
            rollbacks.incrementAndGet();
        }
    }

    @Override
    public long commit(long txnId) throws IOException{
        byte[] rowKey=getTransactionRowKey(txnId);
        TxnMessage.TxnLifecycleMessage lifecycle=TxnMessage.TxnLifecycleMessage.newBuilder()
                .setTxnId(txnId).setAction(TxnMessage.LifecycleAction.COMMIT).build();

        try(TxnNetworkLayer table = tableFactory.accessTxnNetwork()){
            TxnMessage.ActionResponse response = table.lifecycleAction(rowKey,lifecycle);
            commits.incrementAndGet();
            return response.getCommitTs();
        }
    }

    @Override
    public boolean keepAlive(long txnId) throws IOException{
        byte[] rowKey=getTransactionRowKey(txnId);

        TxnMessage.TxnLifecycleMessage lifecycle=TxnMessage.TxnLifecycleMessage.newBuilder()
                .setTxnId(txnId).setAction(TxnMessage.LifecycleAction.KEEPALIVE).build();
        try(TxnNetworkLayer table = tableFactory.accessTxnNetwork()){
            TxnMessage.ActionResponse actionResponse=table.lifecycleAction(rowKey,lifecycle);
            return actionResponse.getContinue();
        }
    }

    @Override
    public void elevateTransaction(Txn txn,byte[] newDestinationTable) throws IOException{
        byte[] rowKey=getTransactionRowKey(txn.getTxnId());
        TxnMessage.ElevateRequest elevateRequest=TxnMessage.ElevateRequest.newBuilder()
                .setTxnId(txn.getTxnId())
                .setNewDestinationTable(ZeroCopyLiteralByteString.wrap(Encoding.encodeBytesUnsorted(newDestinationTable))).build();

        try(TxnNetworkLayer table = tableFactory.accessTxnNetwork()){
//            TxnMessage.TxnLifecycleService service=getLifecycleService(table,rowKey);

//            SpliceRpcController controller=new SpliceRpcController();
//            service.elevateTransaction(controller,elevateRequest,new BlockingRpcCallback<TxnMessage.VoidResponse>());
//            dealWithError(controller);
            table.elevate(rowKey,elevateRequest);
            elevations.incrementAndGet();
        }
    }

    @Override
    public long[] getActiveTransactionIds(Txn txn,byte[] table) throws IOException{
        return getActiveTransactionIds(timestampSource.retrieveTimestamp(),txn.getTxnId(),table);
    }

    @Override
    public long[] getActiveTransactionIds(final long minTxnId,final long maxTxnId,final byte[] writeTable) throws IOException{
        TxnMessage.ActiveTxnRequest.Builder requestBuilder=TxnMessage.ActiveTxnRequest
                .newBuilder().setStartTxnId(minTxnId).setEndTxnId(maxTxnId);
        if(writeTable!=null)
            requestBuilder=requestBuilder.setDestinationTables(ZeroCopyLiteralByteString.wrap(Encoding.encodeBytesUnsorted(writeTable)));

        final TxnMessage.ActiveTxnRequest request=requestBuilder.build();
        try(TxnNetworkLayer table = tableFactory.accessTxnNetwork()){
            return table.getActiveTxnIds(request);
        }catch(Throwable throwable){
            throw new IOException(throwable);
        }
    }

    @Override
    public List<TxnView> getActiveTransactions(final long minTxnid,final long maxTxnId,final byte[] activeTable) throws IOException{
        TxnMessage.ActiveTxnRequest.Builder requestBuilder=TxnMessage.ActiveTxnRequest
                .newBuilder().setStartTxnId(minTxnid).setEndTxnId(maxTxnId);
        if(activeTable!=null)
            requestBuilder=requestBuilder.setDestinationTables(ZeroCopyLiteralByteString.wrap(Encoding.encodeBytesUnsorted(activeTable)));

        final TxnMessage.ActiveTxnRequest request=requestBuilder.build();
        try(TxnNetworkLayer table = tableFactory.accessTxnNetwork()){
            Collection<TxnMessage.ActiveTxnResponse> data = table.getActiveTxns(request);

            List<TxnView> txns=new ArrayList<>(data.size());

            for(TxnMessage.ActiveTxnResponse response : data){
                int size=response.getTxnsCount();
                for(int i=0;i<size;i++){
                    txns.add(decode(0, response.getTxns(i)));
                }
            }
            Collections.sort(txns,new Comparator<TxnView>(){
                @Override
                public int compare(TxnView o1,TxnView o2){
                    if(o1==null){
                        if(o2==null) return 0;
                        else return -1;
                    }else if(o2==null) return 1;
                    return Longs.compare(o1.getTxnId(),o2.getTxnId());
                }
            });
            return txns;

        }catch(Throwable throwable){
            throw new IOException(throwable);
        }
    }

    @Override
    public long getTxnAt(long ts) throws IOException {
        final TxnMessage.TxnAtRequest request=TxnMessage.TxnAtRequest.newBuilder().setTs(ts).build();
        try (TxnNetworkLayer table = tableFactory.accessTxnNetwork()) {
            TxnMessage.TxnAtResponse result = table.getTxnAt(request);
            return result.getTxnId();
        } catch(Throwable throwable) {
            throw new IOException(throwable);
        }
    }

    @Override
    public TxnView getTransaction(long txnId) throws IOException{
        return getTransaction(txnId,false);
    }

    @Override
    public TxnView getTransaction(long txnId,boolean getDestinationTables) throws IOException{
        lookups.incrementAndGet(); //we are performing a lookup, so increment the counter
        if (txnId < oldTransactions) {
            return getOldTransaction(txnId, getDestinationTables);
        }

        byte[] rowKey=getTransactionRowKey(txnId );
        TxnMessage.TxnRequest request=TxnMessage.TxnRequest.newBuilder().setTxnId(txnId).build();

        try (TxnNetworkLayer table = tableFactory.accessTxnNetwork()) {

            TxnMessage.Txn messageTxn = table.getTxn(rowKey, request);
            return decode(txnId, messageTxn);
        } catch (IOException e) {
            throw e;
        } catch(Throwable throwable){
            throw new IOException(throwable);
        }
    }

    public TxnView getOldTransaction(long txnId, boolean getDestinationTables) throws IOException {
        byte[] rowKey = getOldTransactionRowKey(txnId);
        TxnMessage.TxnRequest request = TxnMessage.TxnRequest.newBuilder().setTxnId(txnId).setIsOld(true).build();

        try (TxnNetworkLayer table = tableFactory.accessTxnNetwork()){
            TxnMessage.Txn messageTxn=table.getTxn(rowKey,request);
            return decode(txnId, messageTxn);
        } catch (IOException e) {
            throw e;
        } catch(Throwable throwable){
            throw new IOException(throwable);
        }
    }

    private static byte[] getOldTransactionRowKey(long txnId){
        return TxnUtils.getOldRowKey(txnId);
    }

    @Override
    public void setOldTransactions(long oldTransactions) {
        this.oldTransactions = oldTransactions;
    }

    @Override
    public long getOldTransactions() {
        return oldTransactions;
    }

    /*caching methods--since we don't have a cache, these are no-ops*/
    @Override
    public boolean transactionCached(long txnId){
        return false;
    }

    @Override
    public void cache(TxnView toCache){
    }

    @Override
    public TxnView getTransactionFromCache(long txnId){
        return null;
    }

    @Override
    public TaskId getTaskId(long txnId) throws IOException {
        lookups.incrementAndGet(); //we are performing a lookup, so increment the counter

        byte[] rowKey=getTransactionRowKey(txnId );
        TxnMessage.TxnRequest request=TxnMessage.TxnRequest.newBuilder().setTxnId(txnId).build();

        try (TxnNetworkLayer table = tableFactory.accessTxnNetwork()) {
            TxnMessage.TaskId taskId = table.getTaskId(rowKey, request);
            return new TaskId(taskId.getStageId(), taskId.getPartitionId(), taskId.getTaskAttemptNumber());
        } catch (IOException e) {
            throw e;
        } catch(Throwable throwable){
            throw new IOException(throwable);
        }
    }

    /**
     * Set the underlying transaction cache to use.
     * <p/>
     * This allows us to provide a transaction cache when looking up parent transactions,
     * which will reduce some of the cost of looking up a transaction.
     *
     * @param cacheStore the caching store to use
     */
    public void setCache(TxnSupplier cacheStore){
        this.cache=cacheStore;
    }

    /*monitoring methods*/
    public long lookupCount(){
        return lookups.get();
    }

    public long elevationCount(){
        return elevations.get();
    }

    public long createdCount(){
        return txnsCreated.get();
    }

    public long rollbackCount(){
        return rollbacks.get();
    }

    public long commitCount(){
        return commits.get();
    }

    /**
     * **************************************************************************************************************
     */
        /*private helper methods*/
    private TxnView decode(long queryId, TxnMessage.Txn message) throws IOException{
        TxnMessage.TxnInfo info=message.getInfo();
        if(info.getTxnId()<0) {
            // we didn't find it
            if (ignoreMissingTransactions) {
                // return committed mock transaction
                return new InheritingTxnView(Txn.ROOT_TRANSACTION, queryId, queryId,
                        Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                        false, false,
                        true, true,
                        queryId, queryId,
                        Txn.State.COMMITTED, Iterators.emptyIterator(), System.currentTimeMillis(), null);
            }
            else if (SIDriver.driver().lifecycleManager().getReplicationRole().compareToIgnoreCase(SIConstants.REPLICATION_ROLE_REPLICA) == 0) {
                // return active mock transaction
                return new InheritingTxnView(Txn.ROOT_TRANSACTION, queryId, queryId,
                        Txn.IsolationLevel.SNAPSHOT_ISOLATION,
                        false, false,
                        true, true,
                        queryId, queryId,
                        Txn.State.ACTIVE, Iterators.emptyIterator(), System.currentTimeMillis(), null);
            } else {
                 throw new TransactionMissing(queryId);
            }
        }

        long txnId=info.getTxnId();
        long parentTxnId=info.getParentTxnid();
        long beginTs=info.getBeginTs();

        Txn.IsolationLevel isolationLevel=Txn.IsolationLevel.fromInt(info.getIsolationLevel());

        boolean hasAdditive=info.hasIsAdditive();
        boolean additive=hasAdditive && info.getIsAdditive();

        long commitTs=message.getCommitTs();
        long globalCommitTs=message.getGlobalCommitTs();

        Txn.State state=Txn.State.fromInt(message.getState());

        TaskId taskId = null;
        if (info.hasTaskId()) {
            TxnMessage.TaskId ti = info.getTaskId();
            taskId = new TaskId(ti.getStageId(), ti.getPartitionId(), ti.getTaskAttemptNumber());
        }

        Iterator<ByteSlice> destinationTablesIterator = V2TxnDecoder.decodeDestinationTables(info.getDestinationTables());

        long kaTime=-1l;
        if(message.hasLastKeepAliveTime())
            kaTime=message.getLastKeepAliveTime();

        TxnView parentTxn=parentTxnId<0?Txn.ROOT_TRANSACTION:cache.getTransaction(parentTxnId);
        return new InheritingTxnView(parentTxn,txnId,beginTs,
                isolationLevel,
                hasAdditive,additive,
                true,true,
                commitTs,globalCommitTs,
                state, destinationTablesIterator,kaTime,taskId);
    }

    private byte[] encode(Txn txn){
        List<ByteSlice> destinationTables=Lists.newArrayList(txn.getDestinationTables());
        MultiFieldEncoder encoder=MultiFieldEncoder.create(8+destinationTables.size());
        encoder.encodeNext(txn.getTxnId());

        TxnView parentTxn=txn.getParentTxnView();
        if(parentTxn!=null && parentTxn.getTxnId()>=0)
            encoder=encoder.encodeNext(parentTxn.getTxnId());
        else encoder.encodeEmpty();

        encoder.encodeNext(txn.getBeginTimestamp())
                .encodeNext(txn.getIsolationLevel().encode())
                .encodeNext(txn.isAdditive());
        if(txn.getState()==Txn.State.COMMITTED){
            encoder=encoder.encodeNext(txn.getCommitTimestamp());
        }else{
            encoder.encodeEmpty();
        }
        /*
         * We only use this method if we are recording a new transaction. Because of that, we leave
         * off the effectiveCommitTimestamp(). Likely, we wouldn't use it anyway, because we don't have one
         * yet, but on the off chance that we do, we'll let the Transaction Resolver on the coprocessor
         * side handle it.
         *
         * However, we need this in place because we use the same encoding/decoding strategy in multiple
         * places, so we have to adhere to the same policy
         */
        encoder.encodeEmpty();

        encoder.encodeNext(txn.getState().getId());


        //encode the destination tables
        for(ByteSlice destTable : destinationTables){
            encoder=encoder.encodeNextUnsorted(destTable);
        }

        return encoder.build();
    }

    private static byte[] getTransactionRowKey(long txnId){
        return TxnUtils.getRowKey(txnId);
    }

    private void dealWithError(ServerRpcController controller) throws IOException{
        if(!controller.failed()) return; //nothing to worry about
        throw controller.getFailedOn();
    }

    public static void main(String... args) throws Exception{
        System.out.println(Bytes.toStringBinary(getTransactionRowKey(4063485)));
    }
}
