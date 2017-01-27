/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.storage;

import org.spark_project.guava.base.Function;
import com.splicemachine.si.impl.HRegionTooBusy;

import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.spark_project.guava.collect.Iterators;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.HNotServingRegion;
import com.splicemachine.si.impl.HWrongRegion;
import com.splicemachine.storage.util.MeasuredListScanner;
import com.splicemachine.utils.Pair;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * Representation of a single HBase Region as a Partition.
 * <p/>
 * A lot of these methods shouldn't (and generally won't) be called directly, because they will
 * result in an excessive number of objects being created. For example, instead of relying on
 * {@link #get(DataGet,DataResult)} to get the latest version of all the columns, instead call
 * {@link #getLatest(byte[],DataResult)}, which will save on the creation of a DataGet object, while still
 * allowing re-use of the DataResult object.
 * <p/>
 * Nonetheless, all methods are properly implemented in a Thread-safe manner (they must, in order to pass SI
 * acceptance tests).
 *
 * @author Scott Fines
 *         Date: 12/17/15
 */
@ThreadSafe
public class RegionPartition implements Partition{
    private final HRegion region;

    public RegionPartition(HRegion region){
        this.region=region;
    }

    @Override
    public String getTableName(){
        return region.getTableDesc().getTableName().getQualifierAsString();
    }

    @Override
    public String getName(){
        return region.getRegionInfo().getRegionNameAsString();
    }


    @Override
    public Iterator<DataResult> batchGet(Attributable attributes,List<byte[]> rowKeys) throws IOException{
        List<Result> results=new ArrayList<>(rowKeys.size());
        try{
            for(byte[] rk : rowKeys){
                Get g=new Get(rk);
                if(attributes!=null){
                    for(Map.Entry<String, byte[]> attrEntry : attributes.allAttributes().entrySet()){
                        g.setAttribute(attrEntry.getKey(),attrEntry.getValue());
                    }
                }
                results.add(region.get(g));
            }
        }catch(NotServingRegionException nsre){
            throw new HNotServingRegion(nsre.getMessage());
        }catch(WrongRegionException wre){
            throw new HWrongRegion(wre.getMessage());
        }
        final HResult result=new HResult();
        return Iterators.transform(results.iterator(),new Function<Result, DataResult>(){
            @Override
            public DataResult apply(Result input){
                result.set(input);
                return result;
            }
        });
    }

    @Override
    public boolean checkAndPut(byte[] key,byte[] family,byte[] qualifier,byte[] expectedValue,DataPut put) throws IOException{
        return false;
    }

    /*Lifecycle management*/
    @Override
    public void startOperation() throws IOException{
        region.startRegionOperation();
    }

    @Override
    public void closeOperation() throws IOException{
        region.closeRegionOperation();
    }

    @Override
    public boolean isClosed(){
        return region.isClosed();
    }

    @Override
    public boolean isClosing(){
        return region.isClosing();
    }

    @Override
    public void close() throws IOException{
        //no-op for regions
    }

    /*Single row access*/
    @Override
    public DataResult get(DataGet get,DataResult previous) throws IOException{
        assert get instanceof HGet:"Programmer error: improper type!";

        try{
            Result result=region.get(((HGet)get).unwrapDelegate());
            if(previous==null)
                previous=new HResult();
            ((HResult)previous).set(result);
            return previous;
        }catch(NotServingRegionException nsre){
            throw new HNotServingRegion(nsre.getMessage());
        }catch(WrongRegionException wre){
            throw new HWrongRegion(wre.getMessage());
        }
    }

    @Override
    public DataResult getFkCounter(byte[] key,DataResult previous) throws IOException{
        Get g=new Get(key);
        g.addColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES);

        try{
            Result r=region.get(g);
            if(previous==null)
                previous=new HResult(r);
            else{
                ((HResult)previous).set(r);
            }
            return previous;
        }catch(NotServingRegionException nsre){
            throw new HNotServingRegion(nsre.getMessage());
        }catch(WrongRegionException wre){
            throw new HWrongRegion(wre.getMessage());
        }
    }

    @Override
    public DataResult getLatest(byte[] key,DataResult previous) throws IOException{
        Get g=new Get(key);
        g.setMaxVersions(1);

        try{
            Result result=region.get(g);
            if(previous==null)
                previous=new HResult(result);
            else{
                ((HResult)previous).set(result);
            }
            return previous;
        }catch(NotServingRegionException | AssertionError | NullPointerException nsre){
            throw new HNotServingRegion(nsre.getMessage());
        }catch(WrongRegionException wre){
            throw new HWrongRegion(wre.getMessage());
        }
    }

    @Override
    public DataResult getLatest(byte[] rowKey,byte[] family,DataResult previous) throws IOException{
        Get g=new Get(rowKey);
        g.setMaxVersions(1);
        g.addFamily(family);

        try{
            Result result=region.get(g);
            if(previous==null)
                previous=new HResult(result);
            else{
                ((HResult)previous).set(result);
            }
            return previous;
        }catch(NotServingRegionException nsre){
            throw new HNotServingRegion(nsre.getMessage());
        }catch(WrongRegionException wre){
            throw new HWrongRegion(wre.getMessage());
        }
    }

    /*Multi-row access*/
    @Override
    public DataScanner openScanner(DataScan scan) throws IOException{
        return openScanner(scan,Metrics.noOpMetricFactory());
    }

    @Override
    public DataScanner openScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        assert scan instanceof HScan:"Programmer error: improper type!";

        Scan s=((HScan)scan).unwrapDelegate();
        try{
            RegionScanner scanner=region.getScanner(s);

            return new RegionDataScanner(this,scanner,metricFactory);
        }catch(NotServingRegionException nsre){
            throw new HNotServingRegion(nsre.getMessage());
        }catch(WrongRegionException wre){
            throw new HWrongRegion(wre.getMessage());
        }
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan) throws IOException{
        return openResultScanner(scan,Metrics.noOpMetricFactory());
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        assert scan instanceof HScan:"Programmer error: improper type!";

        Scan s=((HScan)scan).unwrapDelegate();
        try{
            RegionScanner scanner=region.getScanner(s);

            //TODO -sf- massage the batch size properly
            return new RegionResultScanner(s.getBatch(),new MeasuredListScanner(scanner,metricFactory));
        }catch(NotServingRegionException nsre){
            throw new HNotServingRegion(nsre.getMessage());
        }catch(WrongRegionException wre){
            throw new HWrongRegion(wre.getMessage());
        }
    }

    /*Data mutation methods*/
    @Override
    public void put(DataPut put) throws IOException{
        assert put instanceof HPut:"Programmer error: incorrect put type!";

        Put p=((HPut)put).unwrapDelegate();

        try{
            region.put(p);
        }catch(NotServingRegionException nsre){
            throw new HNotServingRegion(nsre.getMessage());
        }catch(WrongRegionException wre){
            throw new HWrongRegion(wre.getMessage());
        }
    }

    @Override
    public Iterator<MutationStatus> writeBatch(DataPut[] toWrite) throws IOException{
        if(toWrite==null || toWrite.length<=0) return Collections.emptyIterator();
        Mutation[] mutations=new Mutation[toWrite.length];

        for(int i=0;i<toWrite.length;i++){
            mutations[i]=((HMutation)toWrite[i]).unwrapHbaseMutation();
        }
        try{
            OperationStatus[] operationStatuses=region.batchMutate(mutations);
            final HMutationStatus resultStatus=new HMutationStatus();
            return Iterators.transform(Iterators.forArray(operationStatuses),new Function<OperationStatus, MutationStatus>(){
                @Override
                public MutationStatus apply(OperationStatus input){
                    resultStatus.set(input);
                    return resultStatus;
                }
            });
        }catch(NotServingRegionException nsre){
            //convert HBase NSRE to Partition-level
            throw new HNotServingRegion(nsre.getMessage());
        }catch(WrongRegionException wre){
            throw new HWrongRegion(wre.getMessage());
        } catch(NullPointerException npe) {
            // Not Setup yet during split
            throw new HRegionTooBusy(npe.getMessage());
        }
    }

    @Override
    public long increment(byte[] rowKey,byte[] family,byte[] qualifier,long amount) throws IOException{
        Increment incr=new Increment(rowKey);
        incr.addColumn(family,qualifier,amount);
        try{
            Result increment=region.increment(incr);
            return Bytes.toLong(increment.value()); //TODO -sf- is this correct?
        }catch(NotServingRegionException nsre){
            throw new HNotServingRegion(nsre.getMessage());
        }catch(WrongRegionException wre){
            throw new HWrongRegion(wre.getMessage());
        }
    }

    @Override
    public void delete(DataDelete delete) throws IOException{
        Delete d=((HDelete)delete).unwrapDelegate();

        try{
            region.delete(d);
        }catch(NotServingRegionException nsre){
            throw new HNotServingRegion(nsre.getMessage());
        }catch(WrongRegionException wre){
            throw new HWrongRegion(wre.getMessage());
        }
    }

    @Override
    public void mutate(DataMutation put) throws IOException{
        try{
            if(put instanceof HPut)
                region.put(((HPut)put).unwrapDelegate());
            else
                region.delete(((HDelete)put).unwrapDelegate());
        }catch(NotServingRegionException nsre){
            throw new HNotServingRegion(nsre.getMessage());
        }catch(WrongRegionException wre){
            throw new HWrongRegion(wre.getMessage());
        }
    }

    @Override
    public Lock getRowLock(byte[] key,int keyOff,int keyLen) throws IOException{
        if(keyOff==0 && keyLen==key.length)
            return new HLock(region,key);
        else return new HLock(region,Bytes.copy(key,keyOff,keyLen));
    }

    /*Data range ownership methods*/
    @Override
    public byte[] getStartKey(){
        return region.getRegionInfo().getStartKey();
    }

    @Override
    public byte[] getEndKey(){
        return region.getRegionInfo().getEndKey();
    }

    @Override
    public boolean containsRow(byte[] row){
        return region.getRegionInfo().containsRow(row);
    }

    @Override
    public boolean containsRow(byte[] row,int offset,int length){
        if(offset==0 && length==row.length)
            return region.getRegionInfo().containsRow(row);
        else
            return region.getRegionInfo().containsRow(Bytes.copy(row,offset,length));
    }

    @Override
    public boolean overlapsRange(byte[] start,byte[] stop){
        return region.getRegionInfo().containsRange(start,stop);
    }

    @Override
    public boolean overlapsRange(byte[] start,int startOff,int startLen,byte[] stop,int stopOff,int stopLen){
        byte[] s;
        byte[] e;
        if(startOff==0 && startLen==start.length)
            s=start;
        else s=Bytes.copy(start,startOff,startLen);

        if(stopOff==0 && stopLen==stop.length)
            e=stop;
        else e=Bytes.copy(stop,stopOff,stopLen);

        return region.getRegionInfo().containsRange(s,e);
    }

    @Override
    public void writesRequested(long writeRequests){
        HRegionUtil.updateWriteRequests(region,writeRequests);
    }

    @Override
    public void readsRequested(long readRequests){
        HRegionUtil.updateReadRequests(region,readRequests);
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
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public List<Partition> subPartitions(byte[] startRow,byte[] stopRow){
        return subPartitions(startRow,stopRow,false);
    }

    @Override
    public List<Partition> subPartitions(byte[] startRow,byte[] stopRow,boolean refresh){
        if(!region.getRegionInfo().containsRange(startRow,stopRow))
            throw new IllegalArgumentException("A RegionPartition cannot be broken into a range that it does not contain!");
        //TODO -sf- convert to a list of subranges?
        return Collections.<Partition>singletonList(this);
    }

    @Override
    public PartitionLoad getLoad() throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void compact() throws IOException{
        //TODO -sf- is this correct?
        region.compactStores();
    }

    /**
     * Synchronously flush the caches.
     *
     * When this method is called the all caches will be flushed unless:
     * <ol>
     *   <li>the cache is empty</li>
     *   <li>the region is closed.</li>
     *   <li>a flush is already in progress</li>
     *   <li>writes are disabled</li>
     * </ol>
     *
     * <p>This method may block for some time, so it should not be called from a
     * time-sensitive thread.
     *
     * @throws IOException general io exceptions
     * @throws DroppedSnapshotException Thrown when replay of wal is required
     * because a Snapshot was not properly persisted.
     */
    @Override
    public void flush() throws IOException{
        HBasePlatformUtils.flush(region);
    }

    public HRegion unwrapDelegate(){
        return region;
    }

    @Override
    public BitSet getBloomInMemoryCheck(boolean hasConstraintChecker,Pair<KVPair, Lock>[] dataAndLocks) throws IOException {
        return HRegionUtil.keyExists(hasConstraintChecker,region.getStore(SIConstants.DEFAULT_FAMILY_BYTES),dataAndLocks);
    }
}
