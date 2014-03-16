package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.STableWriter;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;

public class HTableWriter implements STableWriter<IHTable, Mutation, Put, Delete> {

    @Override
    public void write(IHTable table, Put put) throws IOException {
        table.put(put);
    }

    @Override
    public void write(IHTable table, Put put, Integer rowLock) throws IOException {
        table.put(put, rowLock);
    }

    @Override
    public void write(IHTable table, Put put, boolean durable) throws IOException {
        table.put(put, durable);
    }

    @Override
    public void write(IHTable table, List<Put> puts) throws IOException {
        table.put(puts);
    }

    @Override
    public OperationStatus[] writeBatch(IHTable table, Pair<Mutation, Integer>[] puts) throws IOException {
        return table.batchPut(puts);
    }

    @Override
    public void delete(IHTable table, Delete delete, Integer rowLock) throws IOException {
        table.delete(delete, rowLock);
    }

		@Override
		public Integer tryLock(IHTable ihTable, byte[] rowKey) {
				return ihTable.tryLock(rowKey);
		}

		@Override
    public boolean checkAndPut(IHTable table, byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException {
        return table.checkAndPut(family, qualifier, expectedValue, put);
    }

    @Override
    public Integer lockRow(IHTable table, byte[] rowKey) throws IOException {
        return table.lockRow(rowKey);
    }

    @Override
    public void unLockRow(IHTable table, Integer lock) throws IOException {
        table.unLockRow(lock);
    }
}
