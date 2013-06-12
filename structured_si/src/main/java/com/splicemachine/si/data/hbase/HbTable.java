package com.splicemachine.si.data.hbase;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class HbTable implements IHTable {
    final HTableInterface table;

    public HbTable(HTableInterface table) {
        this.table = table;
    }

    @Override
    public void close() throws IOException {
        table.close();
    }

    @Override
    public Result get(Get get) throws IOException {
        return table.get(get);
    }

    @Override
    public Iterator<Result> scan(Scan scan) throws IOException {
        final ResultScanner scanner = table.getScanner(scan);
        return scanner.iterator();
    }

    @Override
    public void put(Put put) throws IOException {
        table.put(put);
    }

    @Override
    public void put(Put put, HRowLock rowLock) throws IOException {
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
    public boolean checkAndPut(byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException {
        return table.checkAndPut(put.getRow(), family, qualifier, expectedValue, put);
    }

    @Override
    public void delete(Delete delete, HRowLock rowLock) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public HRowLock lockRow(byte[] rowKey) throws IOException {
        return new HRowLock(table.lockRow(rowKey));
    }

    @Override
    public void unLockRow(HRowLock lock) throws IOException {
        table.unlockRow(lock.lock);
    }
}
