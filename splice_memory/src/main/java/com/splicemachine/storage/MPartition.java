package com.splicemachine.storage;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.util.MappedDataResultScanner;
import com.splicemachine.utils.ByteSlice;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MPartition implements Partition<Attributable,MDelete,MGet, MPut,MResult,MScan>{
    private final String partitionName;

    private final ConcurrentSkipListSet<DataCell> memstore = new ConcurrentSkipListSet<>();
    private final BiMap<ByteBuffer,Lock> lockMap =HashBiMap.create();

    public MPartition(String partitionName){
        this.partitionName=partitionName;
    }

    @Override public String getName(){ return partitionName; }
    @Override public void close() throws IOException{ }

    @Override
    public MResult get(MGet mGet) throws IOException{
        return (MResult)get(mGet,null);
    }

    @Override
    public DataResult get(DataGet get,DataResult previous) throws IOException{
        DataCell start = new MCell(get.key(),new byte[]{},new byte[]{},get.highTimestamp(),new byte[]{},CellType.USER_DATA);
        DataCell end = new MCell(get.key(),SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES,get.lowTimestamp(),new byte[]{},CellType.USER_DATA);

        Set<DataCell> data = memstore.subSet(start,true,end,true);
        try(SetScanner ss = new SetScanner(data.iterator(),get.lowTimestamp(),get.highTimestamp(),get.filter())){
            List<DataCell> toReturn=ss.next(-1);
            if(toReturn==null) return null;

            if(previous==null)
                previous = new MResult();
            assert previous instanceof MResult:"Incorrect result type!";
            ((MResult)previous).set(toReturn);

            return previous;
        }
    }

    @Override
    public DataResult getLatest(byte[] rowKey,byte[] family,DataResult previous) throws IOException{
        DataCell start = new MCell(rowKey,family,new byte[]{},Long.MAX_VALUE,new byte[]{},CellType.USER_DATA);
        DataCell end = new MCell(rowKey,family,SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES,0l,new byte[]{},CellType.USER_DATA);

        Set<DataCell> data = memstore.subSet(start,true,end,true);
        List<DataCell> toReturn = new ArrayList<>(data.size());
        DataCell last = null;
        for(DataCell d:data){
            if(last==null){
                toReturn.add(d);
            }else if(d.dataType()!=last.dataType()){
                toReturn.add(d);
            }
            last=d;
        }

        if(previous==null)
            previous = new MResult();
        assert previous instanceof MResult:"Incorrect result type!";
        ((MResult)previous).set(toReturn);

        return previous;
    }

    @Override
    public CloseableIterator<MResult> scan(MScan mScan) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataScanner openScanner(DataScan scan) throws IOException{
        return openScanner(scan,Metrics.noOpMetricFactory());
    }

    @Override
    public DataScanner openScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        DataCell start = new MCell(scan.getStartKey(),new byte[]{},new byte[]{},scan.highVersion(),new byte[]{},CellType.COMMIT_TIMESTAMP);
        DataCell stop = new MCell(scan.getStopKey(),SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES,scan.lowVersion(),new byte[]{},CellType.FOREIGN_KEY_COUNTER);
        NavigableSet<DataCell> dataCells=memstore.subSet(start,true,stop,false);
        return new SetScanner(dataCells.iterator(),scan.lowVersion(),scan.highVersion(),scan.getFilter());
    }

    @Override
    public void put(MPut mPut) throws IOException{
        put(mPut,true);
    }

    @Override
    public void put(MPut mPut,Lock rowLock) throws IOException{
        put(mPut,true);
    }

    @Override
    public void put(MPut mPut,boolean durable) throws IOException{
        Lock lock=getRowLock(mPut.key(),0,mPut.key().length);
        lock.lock();
        try{
            Iterable<DataCell> cells=mPut.cells();
            for(DataCell dc : cells){
                if(memstore.contains(dc)){
                    memstore.remove(dc);
                }
                memstore.add(dc.getClone());
            }
        }finally{
            lock.unlock();
        }
    }

    @Override
    public void put(List<MPut> mPuts) throws IOException{
        for(MPut put:mPuts){
            put(put,true);
        }
    }

    @Override
    public void put(DataPut put) throws IOException{
        assert put instanceof MPut: "Programmer error: was not a memory put!";
        put((MPut)put);
    }

    @Override
    public boolean checkAndPut(byte[] family,byte[] qualifier,byte[] expectedValue,MPut mPut) throws IOException{
        Lock rowLock = getRowLock(mPut.key(),0,mPut.key().length);
        rowLock.lock();
        try{
            DataCell start=new MCell(mPut.key(),family,qualifier,Long.MAX_VALUE,new byte[]{},CellType.USER_DATA);
            DataCell stop=new MCell(mPut.key(),family,qualifier,0l,new byte[]{},CellType.USER_DATA);
            NavigableSet<DataCell> dataCells=memstore.subSet(start,true,stop,true);
            Iterator<DataCell> iter = dataCells.iterator();
            if(iter.hasNext()){
                DataCell dc=iter.next();
                if(Bytes.BASE_COMPARATOR.equals(dc.value(),expectedValue)){
                    put(mPut,rowLock);
                    return true;
                }
            }
            return false;
        }finally{
            rowLock.unlock();
        }
    }

    @Override
    public void delete(MDelete mDelete,Lock rowLock) throws IOException{
        //remove elements from the row
        rowLock.lock();
        try{
            Iterable<DataCell> exactCellsToDelete=mDelete.cells();
            for(DataCell dc:exactCellsToDelete){
                memstore.remove(dc);
            }
            //TODO -sf- make this also remove entire families and columns
        }finally{
            rowLock.unlock();
        }
    }

    @Override
    public void startOperation() throws IOException{

    }

    @Override
    public void closeOperation() throws IOException{

    }

    @Override
    public Iterator<MutationStatus> writeBatch(DataPut[] toWrite) throws IOException{
        List<MutationStatus> status = new ArrayList<>(toWrite.length);
        for(int i=0;i<toWrite.length;i++){
            DataPut dp = toWrite[i];
            assert dp instanceof MPut: "Incorrect put type";
            put((MPut)dp);
            status.add(MOperationStatus.success());
        }
        return status.iterator();
    }

    @Override
    public Lock getLock(byte[] rowKey,boolean waitForLock) throws IOException{
        return getRowLock(rowKey,0,rowKey.length);
    }

    @Override
    public byte[] getStartKey(){
        DataCell first=memstore.first();
        if(first==null) return SIConstants.EMPTY_BYTE_ARRAY;
        else return first.key();
    }

    @Override
    public byte[] getEndKey(){
        DataCell last = memstore.last();
        if(last==null) return SIConstants.EMPTY_BYTE_ARRAY;
        else return last.key();
    }

    @Override
    public void increment(byte[] rowKey,byte[] family,byte[] qualifier,long amount) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override public boolean isClosed(){ return false; }
    @Override public boolean isClosing(){ return false; }

    @Override
    public DataResult getFkCounter(byte[] key,DataResult previous) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataResult getLatest(byte[] key,DataResult previous) throws IOException{
        DataCell s = new MCell(key,new byte[]{},new byte[]{},Long.MAX_VALUE,new byte[]{},CellType.USER_DATA);
        DataCell e = new MCell(key,SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES,0l,new byte[]{},CellType.USER_DATA);

        NavigableSet<DataCell> dataCells=memstore.subSet(s,true,e,true);
        List<DataCell> results = new ArrayList<>(dataCells.size());
        DataCell lastResult = null;
        for(DataCell dc:dataCells){
           if(lastResult==null){
               results.add(dc);
               lastResult=dc;
           }else if(!dc.matchesQualifier(lastResult.family(),lastResult.qualifier())){
               results.add(dc);
               lastResult = dc;
           }
        }
        return new MResult(results);
    }

    @Override
    public Lock getRowLock(byte[] key,int keyOff,int keyLen) throws IOException{
        final ByteBuffer wrap=ByteBuffer.wrap(key,keyOff,keyLen);
        Lock lock;
        synchronized(lockMap){
            lock = lockMap.get(wrap);
            if(lock==null){
                lock = new MemLock(wrap);
                lockMap.put(wrap,lock);
            }
        }
        return lock;
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        return new MappedDataResultScanner(openScanner(scan,metricFactory)){
            @Override protected DataResult newResult(){ return new MResult(); }

            @Override
            protected void setResultRow(List<DataCell> nextRow,DataResult resultWrapper){
                ((MResult)resultWrapper).set(nextRow);
            }
        };
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan) throws IOException{
        return openResultScanner(scan,Metrics.noOpMetricFactory());
    }


    @Override
    public void delete(DataDelete delete) throws IOException{
        assert delete instanceof MDelete: "Programmer error: incorrect delete type for memory access!";
        delete((MDelete)delete,getRowLock(delete.key(),0,delete.key().length));
    }

    @Override
    public void mutate(DataMutation put) throws IOException{
        if(put instanceof DataPut)
            put((DataPut)put);
        else delete((DataDelete)put);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private class MemLock implements Lock{
        private ByteBuffer key;
        private final Lock lock;
        private int lockCount = 0;

        public MemLock(ByteBuffer key){
            this.key=key;
            this.lock = new ReentrantLock();
        }

        @Override public void lock(){
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
            if(lockCount==0)
                lockMap.remove(key);
            lock.unlock();
        }

        @Override
        @Nonnull
        public Condition newCondition(){
            return lock.newCondition();
        }
    }
}
