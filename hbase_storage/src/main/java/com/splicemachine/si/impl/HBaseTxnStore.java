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

import com.splicemachine.si.api.txn.TransactionStore;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.spark_project.guava.collect.Iterators;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Txn Store which uses the TxnLifecycleEndpoint to manage and access transactions
 * remotely.
 * <p/>
 * This class has no local cache. Callers are responsible for caching returned transactions
 * safely.
 *
 * @author Scott Fines
 *         Date: 6/27/14
 */
@ThreadSafe
public class HBaseTxnStore implements TransactionStore {
    private TxnSupplier cache; //a transaction store which uses a global cache for us
    @ThreadSafe
    private final TimestampSource timestampSource;

    /*monitoring fields*/
    private final AtomicLong lookups=new AtomicLong(0l);
    private final AtomicLong elevations=new AtomicLong(0l);
    private final AtomicLong txnsCreated=new AtomicLong(0l);
    private final AtomicLong rollbacks=new AtomicLong(0l);
    private final AtomicLong commits=new AtomicLong(0l);

    public HBaseTxnStore(TimestampSource timestampSource,
                         @ThreadSafe TxnSupplier txnCache){
        this.cache = txnCache==null?this:txnCache; // Not Used...
        this.timestampSource=timestampSource;
    }

    @Override
    public void recordNewTransaction(Txn txn) throws IOException {
        byte[] rowKey=getTransactionRowKey(txn.getTxnId());
        List<byte[]> bytes=null;
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
    public void rollback(long txnId) throws IOException{
        byte[] rowKey=getTransactionRowKey(txnId);
        rollbacks.incrementAndGet();
    }

    @Override
    public long commit(long txnId) throws IOException{
        byte[] rowKey=getTransactionRowKey(txnId);
        commits.incrementAndGet();
        return response.getCommitTs();
    }

    @Override
    public void elevateTransaction(Txn txn) throws IOException{
        byte[] rowKey=getTransactionRowKey(txn.getTxnId());
        elevations.incrementAndGet();
    }

    @Override
    public Txn getTransaction(long txnId) throws IOException{
        return getTransaction(txnId,false);
    }

    @Override
    public Txn getTransaction(long txnId,boolean getDestinationTables) throws IOException{
        lookups.incrementAndGet(); //we are performing a lookup, so increment the counter
        byte[] rowKey=getTransactionRowKey(txnId);
        TxnMessage.TxnRequest request=TxnMessage.TxnRequest.newBuilder().setTxnId(txnId).build();

        try (TxnNetworkLayer table = tableFactory.accessTxnNetwork()){
//            TxnMessage.TxnLifecycleService service=getLifecycleService(table,rowKey);
//            SpliceRpcController controller=new SpliceRpcController();
//            BlockingRpcCallback<TxnMessage.Txn> done=new BlockingRpcCallback<>();
//            service.getTransaction(controller,request,done);
//            dealWithError(controller);
            TxnMessage.Txn messageTxn=table.getTxn(rowKey,request);
            return decode(messageTxn);
        } catch(Throwable throwable){
            throw new IOException(throwable);
        }
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