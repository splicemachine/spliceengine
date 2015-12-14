package com.splicemachine.si.data.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.data.SRowLock;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.Pair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
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
    public SRowLock lockRow(byte[] rowKey) throws IOException {
        return new HRowLock(region.getRowLock(rowKey, true));
    }

    @Override
    public void unLockRow(SRowLock lock) throws IOException {
        List<HRegion.RowLock> locks = new ArrayList<HRegion.RowLock>(1);
        locks.add((HRegion.RowLock)lock.getDelegate()); // Revisit JL-TODO
        region.releaseRowLocks(locks);
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
    public SRowLock getLock(byte[] rowKey, boolean waitForLock) throws IOException {
        HRegion.RowLock rowLock = region.getRowLock(rowKey, waitForLock);
        if(rowLock == null) return null;
        return new HRowLock(rowLock);
    }

    @Override
    public SRowLock tryLock(ByteSlice rowKey) throws IOException {
        //TODO -sf- HBase requires us to make a copy here, can we avoid that?
        return getLock(rowKey.getByteCopy(), false);
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
    public void put(Put put, SRowLock rowLock) throws IOException {
        region.put(put);
    }

    @Override
    public void put(Put put, boolean durable) throws IOException {
        if (!durable)
            put.setDurability(Durability.SKIP_WAL);
        region.put(put);
    }

    @Override
    public OperationStatus[] batchPut(Pair<Mutation, HRowLock>[] puts)
            throws IOException {
        Mutation[] mutations = new Mutation[puts.length];
        int i=0;
        for(Pair<Mutation, HRowLock> pair:puts){
            mutations[i] = pair.getFirst();
            i++;
        }
        return region.batchMutate(mutations);
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
    public void delete(Delete delete, SRowLock rowLock) throws IOException {
        region.delete(delete);
    }

    @Override
    public OperationStatus[] batchMutate(Collection<KVPair> data,TxnView txn) throws IOException {
        Mutation[] mutations = new Mutation[data.size()];
        int i=0;
        for(KVPair pair:data){
            mutations[i] = getMutation(pair,txn);
            i++;
        }
        return region.batchMutate(mutations);
    }

    @Override
    public boolean isClosed() {
        return region.isClosed();
    }

    @Override
    public boolean isClosing() {
        return region.isClosing();
    }
}