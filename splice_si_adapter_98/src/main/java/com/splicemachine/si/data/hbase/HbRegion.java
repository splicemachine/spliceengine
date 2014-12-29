package com.splicemachine.si.data.hbase;

import static com.splicemachine.constants.SpliceConstants.CHECK_BLOOM_ATTRIBUTE_NAME;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.hbase.KVPair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SRowLock;

/**
 * Wrapper that makes an HBase region comply with a standard interface that abstracts across regions and tables.
 */
public class HbRegion extends BaseHbRegion<SRowLock> {
//    static final Logger LOG = Logger.getLogger(HbRegion.class);
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
        final SRowLock lock = new HRowLock(region.getRowLock(rowKey, true));
        if (lock == null) {
            throw new RuntimeException("Unable to obtain row lock on region of table " + region.getTableDesc()
                                                                                               .getNameAsString());
        }
        return lock;
    }

    @Override
    public void unLockRow(SRowLock lock) throws IOException {
        List<HRegion.RowLock> locks = new ArrayList<>(1);
        locks.add(lock.getDelegate());
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
    public SRowLock tryLock(byte[] rowKey) throws IOException {
            HRegion.RowLock rowLock = region.getRowLock(rowKey, false);
            if(rowLock == null) return null;
            return new HRowLock(rowLock);
    }

    private boolean rowExists(byte[] checkBloomFamily, byte[] rowKey) throws IOException {
        return HRegionUtil.keyExists(region.getStore(checkBloomFamily), rowKey);
    }

    private Result emptyResult() {
        return EMPTY_RESULT;
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
	public OperationStatus[] batchPut(Pair<Mutation, SRowLock>[] puts)
			throws IOException {
		Mutation[] mutations = new Mutation[puts.length];
		int i=0;
		for(Pair<Mutation, SRowLock> pair:puts){
				mutations[i] = pair.getFirst();
				i++;
		}
		return region.batchMutate(mutations);
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

}