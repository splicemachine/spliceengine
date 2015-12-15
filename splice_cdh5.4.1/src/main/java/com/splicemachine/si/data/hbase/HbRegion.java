package com.splicemachine.si.data.hbase;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;

import static com.splicemachine.si.constants.SIConstants.CHECK_BLOOM_ATTRIBUTE_NAME;

/**
 * Wrapper that makes an HBase region comply with a standard interface that abstracts across regions and tables.
 */
public class HbRegion extends BaseHbRegion {
    static final Result EMPTY_RESULT = Result.create(Collections.<Cell>emptyList());
    final HRegion region;
    public HbRegion(HRegion region) {
        this.region = region;
    }

    @Override
    public String getName() {
        return region.getTableDesc().getNameAsString();
    }

    @Override
    public void put(DataPut put) throws IOException{
        assert put instanceof HPut: "Programmer error: incorrect type for Put!";
        Put p = ((HPut)put).unwrapDelegate();
        region.put(p);
    }

    @Override
    public DataResult getFkCounter(byte[] key,DataResult previous) throws IOException{
        if(previous==null)
            previous = new HResult();

        Get g = new Get(key);
        g.addColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES);
        assert previous instanceof HResult: "Programmer error: incorrect type for HbTable!";
        ((HResult)previous).set(region.get(g));
        return previous;
    }

    @Override
    public DataResult getLatest(byte[] key,DataResult previous) throws IOException{
        if(previous==null)
            previous = new HResult();

        Get g = new Get(key);
        g.setMaxVersions(1);
        assert previous instanceof HResult: "Programmer error: incorrect type for HbTable!";
        ((HResult)previous).set(region.get(g));
        return previous;
    }

    @Override
    public void close() throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Result get(Get get) throws IOException {
        final byte[] checkBloomFamily = get.getAttribute(CHECK_BLOOM_ATTRIBUTE_NAME);
        if (checkBloomFamily == null || rowExists(checkBloomFamily, get.getRow())) {
            return region.get(get);
        } else {
            return emptyResult();
        }
    }

    @Override
    public CloseableIterator<Result> scan(Scan scan) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void put(Put put) throws IOException {
        region.put(put);
    }

    @Override
    public void put(List<Put> puts) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean checkAndPut(byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException {
        throw new RuntimeException("not implemented");
    }


    @Override
    public void startOperation() throws IOException {
        region.startRegionOperation();
    }

    @Override
    public void closeOperation() throws IOException {
        region.closeRegionOperation();
    }

    @Override
    public Lock getLock(byte[] rowKey, boolean waitForLock) throws IOException {
//        HRegion.RowLock rowLock = region.getRowLock(rowKey, waitForLock);
//        if(rowLock == null) return null;
        return new HLock(region,rowKey);
    }

    @Override
    public Lock getRowLock(ByteSlice byteSlice) throws IOException{
        return new HLock(region,byteSlice.getByteCopy());
    }

    @Override
    public Lock getRowLock(byte[] key,int keyOff,int keyLen) throws IOException{
        byte k [] = new byte[keyLen];
        System.arraycopy(key,keyOff,k,0,keyLen);
        return new HLock(region,k);
    }

    @Override
    public void increment(byte[] rowKey, byte[] family, byte[] qualifier, long amount) throws IOException {
        Increment increment = new Increment(rowKey);
        increment.addColumn(family, qualifier, amount);
        region.increment(increment);
    }

    private boolean rowExists(byte[] checkBloomFamily, byte[] rowKey) throws IOException {
        return HRegionUtil.keyExists(region.getStore(checkBloomFamily), rowKey);
    }

    private Result emptyResult() {
        return EMPTY_RESULT;
    }

    @Override
    protected Put newPut(ByteSlice rowKey) {
        return new Put(rowKey.array(),rowKey.offset(),rowKey.length());
    }

    @Override
    public void put(Put put, Lock rowLock) throws IOException {
        region.put(put);
    }

    @Override
    public void put(Put put, boolean durable) throws IOException {
        if (!durable)
            put.setDurability(Durability.SKIP_WAL);
        region.put(put);
    }

    @Override
    public byte[] getStartKey() {
        return region.getStartKey();
    }

    @Override
    public byte[] getEndKey() {
        return region.getEndKey();
    }

    @Override
    public void delete(Delete delete, Lock rowLock) throws IOException {
        region.delete(delete);
    }

    @Override
    public DataScanner openScanner(DataScan scan) throws IOException{
        return openScanner(scan,Metrics.noOpMetricFactory());
    }

    @Override
    public DataScanner openScanner(DataScan scan,MetricFactory metricFactory) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Iterator<MutationStatus> writeBatch(DataPut[] toWrite) throws IOException{
        Mutation[] mutations = new Mutation[toWrite.length];
        for(int i=0;i<toWrite.length;i++){
            DataPut dp = toWrite[i];
            assert dp instanceof HPut: "Programmer Error: cannot perform batch put with non-hbase cells";
            mutations[i] = ((HPut)dp).unwrapDelegate();
        }

        OperationStatus[] delegates=region.batchMutate(mutations);
        final HMutationStatus wrapper = new HMutationStatus();
        return Iterators.transform(Iterators.forArray(delegates),new Function<OperationStatus, MutationStatus>(){
            @Override
            public MutationStatus apply(OperationStatus input){
                wrapper.set(input);
                return wrapper;
            }
        });
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan,MetricFactory metricFactory){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataResultScanner openResultScanner(DataScan scan){
        return openResultScanner(scan,Metrics.noOpMetricFactory());
    }

    @Override
    public DataResult getLatest(byte[] rowKey,byte[] family,DataResult previous) throws IOException{
        return null;
    }

    @Override
    public void delete(DataDelete delete) throws IOException{
        assert delete instanceof HDelete: "Programmer Error: cannot issue a non-Hbase delete!";
        Delete d=((HDelete)delete).unwrapDelegate();
        region.delete(d);
    }

//    @Override
//    public OperationStatus[] batchMutate(Collection<KVPair> data,TxnView txn) throws IOException {
//        Mutation[] mutations = new Mutation[data.size()];
//        int i=0;
//        for(KVPair pair:data){
//            mutations[i] = getMutation(pair,txn);
//            i++;
//        }
//        return region.batchMutate(mutations);
//    }

    @Override
    public boolean isClosed() {
        return region.isClosed();
    }

    @Override
    public boolean isClosing() {
        return region.isClosing();
    }
}