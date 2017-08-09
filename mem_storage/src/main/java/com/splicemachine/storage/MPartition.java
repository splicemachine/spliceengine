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

package com.splicemachine.storage;

import com.carrotsearch.hppc.LongSet;
import com.splicemachine.access.impl.data.UnsafeRecord;
import com.splicemachine.access.util.ByteComparisons;
import com.splicemachine.si.api.txn.IsolationLevel;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.functions.BatchFetchActiveRecords;
import com.splicemachine.si.impl.functions.ResolveTransaction;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.spark_project.guava.collect.BiMap;
import org.spark_project.guava.collect.HashBiMap;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import org.spark_project.guava.primitives.Longs;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 *
 *
 *
 */
@ThreadSafe
public class MPartition implements Partition<byte[], Txn ,IsolationLevel>{
    private final String partitionName;
    private final String tableName;
    private final PartitionServer owner;
    // One record per key...
    private final ConcurrentSkipListSet<Record<byte[]>> active =new ConcurrentSkipListSet<>(new Comparator<Record<byte[]>>() {
        @Override
        public int compare(Record<byte[]> o1, Record<byte[]> o2) {
            return Bytes.basicByteComparator().compare(o1.getKey(),o2.getKey());
        }
    });
    // N Records Per Key
    private final ConcurrentSkipListSet<Record<byte[]>> redo = new ConcurrentSkipListSet<>(new Comparator<Record<byte[]>>() {
        @Override
        public int compare(Record<byte[]> o1, Record<byte[]> o2) {
            int value = Bytes.basicByteComparator().compare(o1.getKey(),o2.getKey());
            if (value != 0)
                return value;
            return Longs.compare(o1.getTxnId1(),o2.getTxnId1());
        }
    });
    private final HashMap<byte[],Long> incrementMap = new HashMap<byte[],Long>();
    private final BiMap<ByteBuffer, Lock> lockMap=HashBiMap.create();
    private AtomicLong writes=new AtomicLong(0l);
    private AtomicLong reads=new AtomicLong(0l);
    private AtomicLong sequenceGen = new AtomicLong(0l);

    public MPartition(String tableName,String partitionName){
        this.partitionName=partitionName;
        this.tableName=tableName;
        this.owner=new MPartitionServer();
    }

    @Override
    public String getTableName(){
        return tableName;
    }

    @Override
    public String getName(){
        return partitionName;
    }

    @Override
    public void close() throws IOException{
    }

    @Override
    public void startOperation() throws IOException{
    }

    @Override
    public void closeOperation() throws IOException{
    }

    @Override
    public Record get(byte[] key, Txn txn, IsolationLevel isolationLevel) throws IOException {
        Record<byte[]> record = active.floor(new UnsafeRecord(key,0l));
        if (record != null && Bytes.basicByteComparator().compare(record.getKey(),key) == 0) { // Active Match
            Record[] baseRecords =
                    new BatchFetchActiveRecords(1).apply(new SingletonIterator(record));
            LongSet txnsToLookup =
                    new ResolveTransaction(null,null).apply(baseRecords);
            TxnSupplier supplier = null;
            Txn[] txns = supplier.getTransactions(txnsToLookup.toArray());
            TxnSupplier txnCacheSupplier = null;
            txnCacheSupplier.cache(txns);
        }
        return null;
    }

    @Override
    public Iterator<Record> batchGet(List<byte[]>rowKeys, Txn txn, IsolationLevel isolationLevel) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public RecordScanner openScanner(RecordScan scan,Txn txn, IsolationLevel isolationLevel) throws IOException{
        throw new NotSupportedException("not implemented");
        /*
        NavigableSet<Record> dataCells=getAscendingScanSet(scan);
        Iterator<Record> iter = scan.isDescendingScan()? dataCells.descendingIterator(): dataCells.iterator();

        long curSeq = sequenceGen.get();
        return new SetScanner(curSeq,iter,scan.lowVersion(),scan.highVersion(),scan.getFilter(),this,metricFactory);
        */
    }


    @Override
    public void insert(Record record, Txn txn) throws IOException{

    }

    @Override
    public boolean checkAndPut(byte[] key, Record expectedValue, Record record) throws IOException{
        throw new NotSupportedException("not supported");
/*
        Lock lock = getRowLock(key);
        lock.lock();
        try{

            Record latest=getLatest(key,family,null);
            if(latest!=null&& latest.size()>0){
                DataCell dc = latest.latestCell(family,qualifier);
                if(dc!=null){
                    if(ByteComparisons.comparator().compare(dc.valueArray(),dc.valueOffset(),dc.valueLength(),expectedValue,0,expectedValue.length)==0){
                        put(put);
                        return true;
                    }else return false;
                }else{
                    put(put);
                    return true;
                }
            }else{
                put(put);
                return true;
            }
        }finally{
            lock.unlock();
        }
        */
    }

    @Override
    public Iterator<MutationStatus> writeBatch(List<Record> toWrite) throws IOException{
        List<MutationStatus> status=new ArrayList<>(toWrite.size());
        for (Record record: toWrite) {
            status.add(MOperationStatus.success());
        }
        return status.iterator();
    }

    @Override
    public byte[] getStartKey(){
        return SIConstants.EMPTY_BYTE_ARRAY;
    }

    @Override
    public byte[] getEndKey(){
        return SIConstants.EMPTY_BYTE_ARRAY;
    }

    @Override
    public long getCurrentIncrement(byte[] rowKey) throws IOException{
        throw new UnsupportedOperationException("Not Implemented");
    }

    @Override
    public long increment(byte[] rowKey, long amount) throws IOException{
        throw new NotSupportedException("not implemented");
        /*
        Lock lock=getRowLock(rowKey,0,rowKey.length);
        lock.lock();
        try{
            DataResult r=getLatest(rowKey,family,null);
            long v = amount;
            if(r!=null && r.size()>0){
                final DataCell dataCell=r.latestCell(family,qualifier);
                if(dataCell!=null)
                    v += Bytes.toLong(dataCell.value());
            }
            byte[] toWrite=Bytes.toBytes(v);
            MPut p=new MPut(rowKey);
            p.addCell(family,qualifier,toWrite);
            put(p);
            return v;
        }finally{
            lock.unlock();
        }
        */
    }

    @Override
    public boolean isClosed(){
        return false;
    }

    @Override
    public boolean isClosing(){
        return false;
    }

    @Override
    public Lock getRowLock(byte[] key) throws IOException{
        return getRowLock(key,0,key.length);
    }

    @Override
    public Lock getRowLock(byte[] key, int keyOff, int keyLen) throws IOException{
        final ByteBuffer wrap=ByteBuffer.wrap(key,keyOff,keyLen);
        Lock lock;
        synchronized(lockMap){
            lock=lockMap.get(wrap);
            if(lock==null){
                lock=new MemLock(wrap);
                lockMap.put(wrap,lock);
            }
        }
        return lock;
    }

    @Override
    public void delete(byte[] rowKey, Txn txn) throws IOException{

    }

    @Override
    public void mutate(Record record, Txn txn) throws IOException{

    }

    @Override
    public boolean containsKey(byte[] row){
        return true;
    }

    @Override
    public boolean containsKey(byte[] row, long offset, int length){
        return true;
    }

    @Override
    public boolean overlapsKeyRange(byte[] start,byte[] stop){
        return true;
    }

    @Override
    public boolean overlapsKeyRange(byte[] start, long startOffset, int startLength, byte[] stop, long stopOffset, int stopLength){
        return true;
    }

    @Override
    public void writesRequested(long writeRequests){
        writes.addAndGet(writeRequests);
    }

    @Override
    public void readsRequested(long readRequests){
        reads.addAndGet(readRequests);
    }

    @Override
    public List<Partition> subPartitions(){
        return subPartitions(false);
    }

    @Override
    public List<Partition> subPartitions(boolean refresh){
        return Collections.<Partition>singletonList(this);
    }

    @Override
    public PartitionServer owningServer(){
        return owner;
    }

    @Override
    public List<Partition> subPartitions(byte[] startRow,byte[] stopRow, boolean refresh){
        return subPartitions(); //we own everything
    }


    @Override
    public PartitionLoad getLoad() throws IOException{
        return new MPartitionLoad(getName());
    }

    @Override
    public void compact() throws IOException{
        //no-op--memory does not perform compactions
    }

    @Override
    public void flush() throws IOException{
        //no-op--memory does not perform flush
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private class MemLock implements Lock{
        private ByteBuffer key;
        private final Lock lock;
        private int lockCount=0;

        public MemLock(ByteBuffer key){
            this.key=key;
            this.lock=new ReentrantLock();
        }

        @Override
        public void lock(){
            lock.lock();
            lockCount++;
        }

        @Override
        public void lockInterruptibly() throws InterruptedException{
            lock.lockInterruptibly();
            lockCount++;
        }

        @Override
        public boolean tryLock(){
            if(lock.tryLock()){
                lockCount++;
                return true;
            }else return false;
        }

        @Override
        public boolean tryLock(long time,@Nonnull TimeUnit unit) throws InterruptedException{
            if(lock.tryLock(time,unit)){
                lockCount++;
                return true;
            }else return false;
        }

        @Override
        public void unlock(){
            lockCount--;
            if(lockCount==0){
                synchronized(lockMap){
                    lockMap.remove(key);
                }
            }
            lock.unlock();
        }

        @Override
        @Nonnull
        public Condition newCondition(){
            return lock.newCondition();
        }
    }

    @Override
    public BitSet getBloomInMemoryCheck(Record[] records, Lock[] locks) throws IOException {
        return null;
    }
}
