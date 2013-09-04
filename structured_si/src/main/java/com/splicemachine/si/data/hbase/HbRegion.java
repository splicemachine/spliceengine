package com.splicemachine.si.data.hbase;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;

import com.splicemachine.constants.SIConstants;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Wrapper that makes an HBase region comply with a standard interface that abstracts across regions and tables.
 */
public class HbRegion implements IHTable {
    final HRegion region;

    public HbRegion(HRegion region) {
        this.region = region;
    }

    @Override
    public void close() throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Result get(Get get) throws IOException {
        return region.get(get);
    }

    @Override
    public Iterator<Result> scan(Scan scan) throws IOException {
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
    public void put(Put put) throws IOException {
        region.put(put);
    }

    @SuppressWarnings("deprecation")
	@Override
    public void put(Put put, Integer rowLock) throws IOException {
        region.put(put, rowLock);
    }

    @Override
    public void put(Put put, boolean durable) throws IOException {
        region.put(put, durable);
    }

    @Override
    public void put(List<Put> puts) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public OperationStatus[] batchPut(Pair<Mutation, Integer>[] puts) throws IOException {
        return region.batchMutate(puts);
    }

    @Override
    public boolean checkAndPut(byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @SuppressWarnings("deprecation")
	@Override
    public void delete(Delete delete, Integer rowLock) throws IOException {
        region.delete(delete, rowLock, true);
    }

    @Override
    public Integer lockRow(byte[] rowKey) throws IOException {
        final Integer lock = region.obtainRowLock(rowKey);
        if (lock == null) {
            throw new RuntimeException("Unable to obtain row lock on region of table " + region.getTableDesc().getNameAsString());
        }
        return lock;
    }

    @Override
    public void unLockRow(Integer lock) throws IOException {
        region.releaseRowLock(lock);
    }
    @Override
    public Result volatileGet(Get get) throws IOException {
    	List<KeyValue> keyValues = new ArrayList<KeyValue>();
    	if (!HRegionUtil.keyExists(region, region.getStore(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES), get.getRow()))
    			return new Result(keyValues);
    	HRegionUtil.populateKeyValues(region, keyValues, get);
    	return new Result(keyValues);
    }

}
