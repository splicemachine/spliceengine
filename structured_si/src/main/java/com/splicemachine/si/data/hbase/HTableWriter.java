package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.IHTable;
import com.splicemachine.si.data.api.STableWriter;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;
import java.io.IOException;
import java.util.List;

public class HTableWriter<RowLock> implements STableWriter<RowLock,IHTable<RowLock>, Mutation, Put, Delete> {

    @Override
    public void write(IHTable<RowLock> table, Put put) throws IOException {
        table.put(put);
    }

    @Override
    public void write(IHTable<RowLock> table, Put put, RowLock rowLock) throws IOException {
        table.put(put, rowLock);
    }

    @Override
    public void write(IHTable<RowLock> table, Put put, boolean durable) throws IOException {
        table.put(put, durable);
    }

    @Override
    public void write(IHTable<RowLock> table, List<Put> puts) throws IOException {
        table.put(puts);
    }

    @Override
    public OperationStatus[] writeBatch(IHTable<RowLock> table, Pair<Mutation, RowLock>[] puts) throws IOException {
        return table.batchPut(puts);
    }

    @Override
    public void delete(IHTable<RowLock> table, Delete delete, RowLock rowLock) throws IOException {
        table.delete(delete, rowLock);
    }

		@Override
		public RowLock tryLock(IHTable<RowLock> ihTable, byte[] rowKey) throws IOException{
				return ihTable.tryLock(rowKey);
		}

		@Override
    public boolean checkAndPut(IHTable<RowLock> table, byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException {
        return table.checkAndPut(family, qualifier, expectedValue, put);
    }

    @Override
    public RowLock lockRow(IHTable<RowLock> table, byte[] rowKey) throws IOException {
        return table.lockRow(rowKey);
    }

    @Override
    public void unLockRow(IHTable<RowLock> table, RowLock lock) throws IOException {
        table.unLockRow(lock);
    }
}
