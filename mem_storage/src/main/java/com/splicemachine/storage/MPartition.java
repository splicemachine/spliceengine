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

import com.splicemachine.access.util.ByteComparisons;
import com.splicemachine.si.api.txn.IsolationLevel;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.data.SimpleRecord;
import com.splicemachine.si.impl.functions.BatchFetchActiveRecords;
import com.splicemachine.si.impl.functions.CreateLookupSet;
import com.splicemachine.si.impl.functions.ResolveTransaction;
import org.apache.commons.collections.iterators.SingletonIterator;
import org.spark_project.guava.collect.BiMap;
import org.spark_project.guava.collect.HashBiMap;
import org.spark_project.guava.collect.Sets;
import com.splicemachine.collections.EmptyNavigableSet;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import org.spark_project.guava.primitives.Longs;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
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
    private final ConcurrentSkipListSet<Record<byte[],Object[]>> active =new ConcurrentSkipListSet<>(new Comparator<Record<byte[],Object[]>>() {
        @Override
        public int compare(Record<byte[],Object[]> o1, Record<byte[],Object[]> o2) {
            return Bytes.basicByteComparator().compare(o1.getKey(),o2.getKey());
        }
    });
    // N Records Per Key
    private final ConcurrentSkipListSet<Record<byte[],Object[]>> redo = new ConcurrentSkipListSet<>(new Comparator<Record<byte[],Object[]>>() {
        @Override
        public int compare(Record<byte[],Object[]> o1, Record<byte[],Object[]> o2) {
            int value = Bytes.basicByteComparator().compare(o1.getKey(),o2.getKey());
            if (value != 0)
                return value;
            return Longs.compare(o1.getTxnId1(),o2.getTxnId1());
        }
    });
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
        Record<byte[],Object[]> record = active.floor(new MRecord(key));
        if (record != null && Bytes.basicByteComparator().compare(record.getKey(),key) == 0) { // Active Match
            Record[] baseRecords =
                    new BatchFetchActiveRecords(1).apply(new SingletonIterator(record));
            Record[] resolvedRecords =
                    new ResolveTransaction(null,null).apply(baseRecords);

            long[] recordsToLookup = new CreateLookupSet(null,null).apply(resolvedRecords);



        }
        // Active Miss
        else {
            return null;
        }

        DataCell start=new MCell(get.key(),new byte[]{},new byte[]{},get.highTimestamp(),new byte[]{},CellType.USER_DATA);

        Set<DataCell> data=Sets.filter(memstore.tailSet(start,true),new Predicate<DataCell>(){
            @Override
            public boolean apply(DataCell dataCell){
                byte[] key=get.key();
                return Bytes.equals(dataCell.keyArray(),dataCell.keyOffset(),dataCell.keyLength(),key,0,key.length);
            }
        });
        long curSeq = sequenceGen.get();
        try(SetScanner ss=new SetScanner(curSeq,data.iterator(),get.lowTimestamp(),get.highTimestamp(),get.filter(),this,Metrics.noOpMetricFactory())){
            List<DataCell> toReturn=ss.next(-1);
            if(toReturn.size()<=0) return null;

            filterByFamilies(toReturn,get.familyQualifierMap());
            if(toReturn.size()<=0) return null;
            if(previous==null)
                previous=new MResult();
            assert previous instanceof MResult:"Incorrect result type!";
            ((MResult)previous).set(toReturn);

            return previous;
        }
    }

    @Override
    public Iterator<Record> batchGet(List<byte[]>rowKeys, Txn txn, IsolationLevel isolationLevel) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Record getLatest(byte[] rowKey) throws IOException{
        DataCell start=new MCell(rowKey,family,new byte[]{},Long.MAX_VALUE,new byte[]{},CellType.USER_DATA);
        DataCell end=new MCell(rowKey,family,SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES,0l,new byte[]{},CellType.USER_DATA);

        Set<DataCell> data=memstore.subSet(start,true,end,true);
        List<DataCell> toReturn=new ArrayList<>(data.size());
        DataCell last=null;
        for(DataCell d : data){
            if(last==null){
                toReturn.add(d);
            }else if(d.dataType()!=last.dataType()){
                toReturn.add(d);
            }
            last=d;
        }

        if(previous==null)
            previous=new MResult();
        assert previous instanceof MResult:"Incorrect result type!";
        ((MResult)previous).set(toReturn);

        return previous;
    }

    @Override
    public RecordScanner openScanner(RecordScan scan,Txn txn, IsolationLevel isolationLevel) throws IOException{
        NavigableSet<DataCell> dataCells=getAscendingScanSet(scan);
        Iterator<DataCell> iter = scan.isDescendingScan()? dataCells.descendingIterator(): dataCells.iterator();

        long curSeq = sequenceGen.get();
        return new SetScanner(curSeq,iter,scan.lowVersion(),scan.highVersion(),scan.getFilter(),this,metricFactory);
    }


    @Override
    public void insert(Record record, Txn txn) throws IOException{

    }

    @Override
    public boolean checkAndPut(byte[] key, Record expectedValue, Record record) throws IOException{
        Lock lock = getRowLock(key,0,key.length);
        lock.lock();
        try{
            DataResult latest=getLatest(key,family,null);
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
    }

    @Override
    public Iterator<MutationStatus> writeBatch(List<Record> toWrite) throws IOException{
        List<MutationStatus> status=new ArrayList<>(toWrite.length);
        //noinspection ForLoopReplaceableByForEach
        for(int i=0;i<toWrite.length;i++){
            DataPut dp=toWrite[i];
            assert dp instanceof MPut:"Incorrect put type";
            put((MPut)dp);
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
    public long increment(byte[] rowKey, long amount) throws IOException{
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
    public Record getLatest(byte[] key) throws IOException{
        DataCell s=new MCell(key,new byte[]{},new byte[]{},Long.MAX_VALUE,new byte[]{},CellType.USER_DATA);
        DataCell e=new MCell(key,SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES,0l,new byte[]{},CellType.USER_DATA);

        NavigableSet<DataCell> dataCells=memstore.subSet(s,true,e,true);
        List<DataCell> results=new ArrayList<>(dataCells.size());
        DataCell lastResult=null;
        for(DataCell dc : dataCells){
            if(lastResult==null){
                results.add(dc);
                lastResult=dc;
            }else if(!dc.matchesQualifier(lastResult.family(),lastResult.qualifier())){
                results.add(dc);
                lastResult=dc;
            }
        }
        return new MResult(results);
    }

    @Override
    public Lock getRowLock(byte[] key) throws IOException{
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
        assert delete instanceof MDelete:"Programmer error: incorrect delete type for memory access!";
        delete((MDelete)delete,getRowLock(delete.key(),0,delete.key().length));
    }

    @Override
    public void mutate(Record record, Txn txn) throws IOException{
        if(put instanceof DataPut)
            put((DataPut)put);
        else delete((DataDelete)put);
    }

    @Override
    public boolean containsKey(byte[] row){
        return true;
    }

    @Override
    public boolean overlapsKeyRange(byte[] start,byte[] stop){
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

    private void put(MPut mPut) throws IOException{
        long seq = sequenceGen.incrementAndGet();
        Lock lock=getRowLock(mPut.key(),0,mPut.key().length);
        lock.lock();
        try{
            Iterable<DataCell> cells=mPut.cells();
            for(DataCell dc : cells){
                if(memstore.contains(dc)){
                    memstore.remove(dc);
                }
                DataCell clone=dc.getClone();
                ((MCell)clone).sequence(seq);
                memstore.add(clone);
            }
        }finally{
            lock.unlock();
        }
    }

    private void delete(MDelete mDelete,Lock rowLock) throws IOException{
        //remove elements from the row
        rowLock.lock();
        try{
            Iterable<DataCell> exactCellsToDelete=mDelete.cells();
            for(DataCell dc : exactCellsToDelete){
                memstore.remove(dc);
            }
            //TODO -sf- make this also remove entire families and columns
        }finally{
            rowLock.unlock();
        }
    }

    private NavigableSet<DataCell> getAscendingScanSet(RecordScan scan){
        NavigableSet<DataCell> dataCells;
        if(memstore.size()<=0)
            dataCells = EmptyNavigableSet.instance();
        else{
            byte[] startKey=scan.getStartKey();
            byte[] stopKey=scan.getStopKey();
            DataCell start;
            DataCell stop;
            if(startKey==null|| startKey.length==0) {
                if(stopKey==null||stopKey.length==0){
                    return memstore;
                }else{
                    start = memstore.first();
                }
            }else
                start=new MCell(startKey,new byte[]{},new byte[]{},scan.highVersion(),new byte[]{},CellType.COMMIT_TIMESTAMP);

            if(stopKey==null||stopKey.length==0){
                return memstore.tailSet(start,true);
            }else
                stop=new MCell(stopKey,SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES,scan.lowVersion(),new byte[]{},CellType.FOREIGN_KEY_COUNTER);

            /*
             * It is possible (particularly if the start key is null) that the stop value compares to less than
             * the start key, and that is a reasonable situation. In that case, we know that the end results
             * are empty, so bypass creating a new Set object in this case.
             */
            if(stop.compareTo(start)<0) return EmptyNavigableSet.instance();
            dataCells=memstore.subSet(start,true,stop,false);
        }
        return dataCells;
    }


    private void filterByFamilies(List<DataCell> toReturn,Map<byte[], ? extends Set<byte[]>> familyQualifierMap){
        if(familyQualifierMap==null||familyQualifierMap.size()<=0) return;
        Iterator<DataCell> dcIter = toReturn.iterator();
        while(dcIter.hasNext()){
            DataCell dc = dcIter.next();
            boolean foundFamily = false;
            for(Map.Entry<byte[],? extends Set<byte[]>> familyQuals:familyQualifierMap.entrySet()){
               if(dc.matchesFamily(familyQuals.getKey())){
                   foundFamily = true;
                   Set<byte[]> quals = familyQuals.getValue();
                   if(quals.size()>0){
                       boolean foundQual = false;
                       for(byte[] qual:quals){
                           if(dc.matchesQualifier(familyQuals.getKey(),qual)){
                               foundQual=true;
                               break;
                           }
                       }
                       if(!foundQual){
                            dcIter.remove();
                       }
                   }
                   break;
               }
            }
            if(!foundFamily)
                dcIter.remove();
        }
    }

    @Override
    public BitSet getBloomInMemoryCheck(Record[] records, Lock[] locks) throws IOException {
        return null;
    }
}
