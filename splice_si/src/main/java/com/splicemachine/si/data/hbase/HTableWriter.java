package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.IHTable;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;

public class HTableWriter<HbRowLock> implements STableWriter<HbRowLock, IHTable<HbRowLock>, Mutation, Put, Delete> {

    @Override
    public void write(IHTable<HbRowLock> table, Put put) throws IOException {
        table.put(put);
    }

    @Override
    public void write(IHTable<HbRowLock> table, Put put, HbRowLock rowLock) throws IOException {
        table.put(put, rowLock);
    }

    @Override
    public void write(IHTable<HbRowLock> table, Put put, boolean durable) throws IOException {
        table.put(put, durable);
    }

    @Override
    public void write(IHTable<HbRowLock> table, List<Put> puts) throws IOException {
        table.put(puts);
    }

    @Override
    public OperationStatus[] writeBatch(IHTable<HbRowLock> table, Pair<Mutation, HbRowLock>[] puts) throws IOException {
        return table.batchPut(puts);
    }

    @Override
    public void delete(IHTable<HbRowLock> table, Delete delete, HbRowLock rowLock) throws IOException {
        table.delete(delete, rowLock);
    }

    @Override
    public HbRowLock tryLock(IHTable<HbRowLock> ihTable, byte[] rowKey) throws IOException {
        return ihTable.tryLock(rowKey);
    }

    @Override
    public HbRowLock tryLock(IHTable<HbRowLock> ihTable, ByteSlice rowKey) throws IOException {
        return ihTable.tryLock(rowKey);
    }

    @Override
    public boolean checkAndPut(IHTable<HbRowLock> table, byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException {
        return table.checkAndPut(family, qualifier, expectedValue, put);
    }

    @Override
    public HbRowLock lockRow(IHTable<HbRowLock> table, byte[] rowKey) throws IOException {
        return table.lockRow(rowKey);
    }

    @Override
    public void unLockRow(IHTable<HbRowLock> table, HbRowLock lock) throws IOException {
        table.unLockRow(lock);
    }
}
