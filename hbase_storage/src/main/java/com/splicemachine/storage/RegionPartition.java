package com.splicemachine.storage;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.HNotServingRegion;import com.splicemachine.si.impl.HWrongRegion;import com.splicemachine.storage.util.MeasuredListScanner;
import org.apache.hadoop.hbase.NotServingRegionException;import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.Lock;

/**
 * Representation of a single HBase Region as a Partition.
 *
 * A lot of these methods shouldn't (and generally won't) be called directly, because they will
 * result in an excessive number of objects being created. For example, instead of relying on
 * {@link #get(DataGet,DataResult)} to get the latest version of all the columns, instead call
 * {@link #getLatest(byte[], DataResult)}, which will save on the creation of a DataGet object, while still
 * allowing re-use of the DataResult object.
 *
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
        return region.getRegionNameAsString();
    }


    @Override
    public Iterator<DataResult> batchGet(Attributable attributes,List<byte[]> rowKeys) throws IOException{
        List<Result> results = new ArrayList<>(rowKeys.size());
        for(byte[] rk:rowKeys){
            Get g = new Get(rk);
            if(attributes!=null){
                for(Map.Entry<String, byte[]> attrEntry : attributes.allAttributes().entrySet()){
                    g.setAttribute(attrEntry.getKey(),attrEntry.getValue());
                }
            }
            results.add(region.get(g));
        }
        final HResult result = new HResult();
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
    @Override public void startOperation() throws IOException{ region.startRegionOperation(); }
    @Override public void closeOperation() throws IOException{ region.closeRegionOperation(); }
    @Override public boolean isClosed(){ return region.isClosed(); }
    @Override public boolean isClosing(){ return region.isClosing(); }
    @Override
    public void close() throws IOException{
        //no-op for regions
    }

    /*Single row access*/
    @Override
    public DataResult get(DataGet get,DataResult previous) throws IOException{
        assert get instanceof HGet: "Programmer error: improper type!";

        Result result=region.get(((HGet)get).unwrapDelegate());
        if(previous==null)
            previous = new HResult();
        ((HResult)previous).set(result);
        return previous;
    }

    @Override
    public DataResult getFkCounter(byte[] key,DataResult previous) throws IOException{
        Get g = new Get(key);
        g.addColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES);

        Result r = region.get(g);
        if(previous==null)
            previous = new HResult(r);
        else{
            ((HResult)previous).set(r);
        }
        return previous;
    }

    @Override
    public DataResult getLatest(byte[] key,DataResult previous) throws IOException{
        Get g = new Get(key);
        g.setMaxVersions(1);

        Result result=region.get(g);
        if(previous==null)
            previous = new HResult(result);
        else{
            ((HResult)previous).set(result);
        }
        return previous;
    }

    @Override
    public DataResult getLatest(byte[] rowKey,byte[] family,DataResult previous) throws IOException{
        Get g = new Get(rowKey);
        g.setMaxVersions(1);
        g.addFamily(family);

        Result result=region.get(g);
        if(previous==null)
            previous = new HResult(result);
        else{
            ((HResult)previous).set(result);
        }
        return previous;
    }

    /*Multi-row access*/
    @Override
    public DataScanner openScanner(DataScan scan) throws IOException{
        return openScanner(scan,Metrics.noOpMetricFactory());
    }

    @Override
    public DataScanner openScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        assert scan instanceof HScan: "Programmer error: improper type!";

        Scan s=((HScan)scan).unwrapDelegate();
        RegionScanner scanner=region.getScanner(s);

        return new RegionDataScanner(this,scanner,metricFactory);
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan) throws IOException{
        return openResultScanner(scan,Metrics.noOpMetricFactory());
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        assert scan instanceof HScan: "Programmer error: improper type!";

        Scan s=((HScan)scan).unwrapDelegate();
        RegionScanner scanner=region.getScanner(s);

        //TODO -sf- massage the batch size properly
        return new RegionResultScanner(s.getBatch(),new MeasuredListScanner(scanner,metricFactory));
    }

    /*Data mutation methods*/
    @Override
    public void put(DataPut put) throws IOException{
        assert put instanceof HPut: "Programmer error: incorrect put type!";

        Put p = ((HPut)put).unwrapDelegate();

        region.put(p);
    }

    @Override
    public Iterator<MutationStatus> writeBatch(DataPut[] toWrite) throws IOException{
        if(toWrite==null||toWrite.length<=0) return Collections.emptyIterator();
        Mutation[] mutations = new Mutation[toWrite.length];

        for(int i=0;i<toWrite.length;i++){
            mutations[i] = ((HMutation)toWrite[i]).unwrapHbaseMutation();
        }
        try{
            OperationStatus[] operationStatuses=region.batchMutate(mutations);
            final HMutationStatus resultStatus = new HMutationStatus();
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
        }
    }

    @Override
    public long increment(byte[] rowKey,byte[] family,byte[] qualifier,long amount) throws IOException{
        Increment incr = new Increment(rowKey);
        incr.addColumn(family,qualifier,amount);
        Result increment=region.increment(incr);
        return Bytes.toLong(increment.value()); //TODO -sf- is this correct?
    }

    @Override
    public void delete(DataDelete delete) throws IOException{
        Delete d = ((HDelete)delete).unwrapDelegate();

        region.delete(d);
    }

    @Override
    public void mutate(DataMutation put) throws IOException{
        if(put instanceof HPut)
            region.put(((HPut)put).unwrapDelegate());
        else
            region.delete(((HDelete)put).unwrapDelegate());
    }

    @Override
    public Lock getRowLock(byte[] key,int keyOff,int keyLen) throws IOException{
        if(keyOff==0 && keyLen==key.length)
            return new HLock(region,key);
        else return new HLock(region,Bytes.copy(key,keyOff,keyLen));
    }

    /*Data range ownership methods*/
    @Override public byte[] getStartKey(){ return region.getStartKey(); }
    @Override public byte[] getEndKey(){ return region.getEndKey(); }

    @Override public boolean containsRow(byte[] row){ return region.getRegionInfo().containsRow(row); }

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
            s = start;
        else s = Bytes.copy(start,startOff,startLen);

        if(stopOff==0 && stopLen==stop.length)
            e = stop;
        else e = Bytes.copy(stop,stopOff,stopLen);

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
    public List<Partition> subPartitions(byte[] startRow,byte[] stopRow, boolean refresh){
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

    public HRegion unwrapDelegate(){
        return region;
    }
}
