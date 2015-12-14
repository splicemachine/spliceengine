package com.splicemachine.si.data.hbase;

import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.collections.ForwardingCloseableIterator;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.data.IHTable;
import com.splicemachine.si.api.data.SRowLock;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.Pair;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Wrapper that makes an HBase table comply with an interface that allows regions and tables to be used in a uniform manner.
 */
public class HbTable implements IHTable<OperationWithAttributes,Delete,
        Get,Mutation,OperationStatus,Pair<Mutation, HRowLock>,
        Put,Result,Scan> {
    final Table table;

    public HbTable(Table table) {
        this.table = table;
    }

    @Override
    public String getName() {
        try {
            return table.getTableDescriptor().getTableName().getNameAsString();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public void close() throws IOException {
        table.close();
    }

    @Override
    public Result get(Get get) throws IOException {
        final Result result = table.get(get);
        if (result.isEmpty()) {
           return null;
        } else {
            return result;
        }
    }

    @Override
    public CloseableIterator<Result> scan(Scan scan) throws IOException {
        if(scan.getStartRow() == null) {
            scan.setStartRow(new byte[]{});
        }
        final ResultScanner scanner = table.getScanner(scan);
				return new ForwardingCloseableIterator<Result>(scanner.iterator()) {
						@Override
						public void close() throws IOException {
							scanner.close();
						}
				};
    }

    @Override
    public void startOperation() throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void closeOperation() throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public HRowLock getLock(byte[] rowKey, boolean waitForLock) {
        try {
            return lockRow(rowKey);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public HRowLock tryLock(ByteSlice rowKey) throws IOException {
        return lockRow(rowKey.getByteCopy());
    }

    @Override
    public void increment(byte[] rowKey, byte[] family, byte[] qualifier, long amount) throws IOException {
        throw new UnsupportedOperationException("Cannot increment with row lock at table level.");
    }

    @Override
    public void put(Put put) throws IOException {
        table.put(put);
    }

    @Override
    public void put(Put put, SRowLock rowLock) throws IOException {
        table.put(put);
    }

    @Override
    public void put(Put put, boolean durable) throws IOException {
        table.put(put);
    }

    @Override
    public void put(List<Put> puts) throws IOException {
        table.put(puts);
    }

    @Override
    public OperationStatus[] batchPut(Pair<Mutation, HRowLock>[] puts) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean checkAndPut(byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException {
        return table.checkAndPut(put.getRow(), family, qualifier, expectedValue, put);
    }

    @Override
    public void delete(Delete delete, SRowLock rowLock) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public HRowLock lockRow(byte[] rowKey) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void unLockRow(SRowLock lock) throws IOException {
        throw new RuntimeException("not implemented");
    }

	@Override
	public OperationStatus[] batchMutate(Collection<KVPair> data, TxnView txn)
			throws IOException {
        throw new RuntimeException("not implemented");
	}

    @Override
    public byte[] getStartKey() {
        return HConstants.EMPTY_START_ROW;
    }

    @Override
    public byte[] getEndKey() {
        return HConstants.EMPTY_END_ROW;
    }

    @Override
    public boolean isClosed() {
        throw new RuntimeException("Not to be called on Table");
    }

    @Override
    public boolean isClosing() {
        throw new RuntimeException("not to be called on table");
    }
}
