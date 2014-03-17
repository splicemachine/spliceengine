package com.splicemachine.si.data.hbase;

import com.splicemachine.utils.CloseableIterator;
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
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import static com.splicemachine.constants.SpliceConstants.CHECK_BLOOM_ATTRIBUTE_NAME;

/**
 * Wrapper that makes an HBase region comply with a standard interface that abstracts across regions and tables.
 */
public class HbRegion implements IHTable {
    static final Logger LOG = Logger.getLogger(HbRegion.class);
    static final Result EMPTY_RESULT = new Result(Collections.<KeyValue>emptyList());
    
    final HRegion region;

    public HbRegion(HRegion region) {
        this.region = region;
    }

    @Override
    public String getName() {
        return region.getTableDesc().getNameAsString();
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

    private boolean rowExists(byte[] checkBloomFamily, byte[] rowKey) throws IOException {
        return HRegionUtil.keyExists(region.getStore(checkBloomFamily), rowKey);
    }

    private Result emptyResult() {
        return EMPTY_RESULT;
    }

    @Override
    public CloseableIterator<Result> scan(Scan scan) throws IOException {
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
		public Integer tryLock(byte[] rowKey) throws IOException {
				return region.getLock(null, rowKey, false);
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

}
