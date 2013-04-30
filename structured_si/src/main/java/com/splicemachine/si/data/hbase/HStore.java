package com.splicemachine.si.data.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RowLock;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class HStore implements IHTableReader, IHTableWriter {
    private final HTableSource tableSource;

    public HStore(HTableSource tableSource) {
        this.tableSource = tableSource;
    }

    @Override
    public HTableInterface open(String tableName) throws IOException {
        return tableSource.getTable(tableName);
    }

    @Override
    public Result get(HTableInterface table, Get get) throws IOException {
        return table.get(get);
    }

    @Override
    public Result get(HRegion region, Get get) throws IOException {
        return region.get(get, null);
    }

    @Override
    public Iterator scan(HTableInterface table, Scan scan) throws IOException {
        final ResultScanner scanner = table.getScanner(scan);
        return scanner.iterator();
    }

    @Override
    public void close(HTableInterface table) throws IOException {
        table.close();
    }

    @Override
    public void write(HTableInterface table, Put put) throws IOException {
        table.put(put);
    }

    @Override
    public void write(HRegion region, Put put) throws IOException {
        region.put(put);
    }

    @Override
    public void write(HRegion region, Put put, Integer lock) throws IOException {
        region.put(put, lock);
    }

    @Override
    public void write(Object table, Put put, boolean durable) throws IOException {
        if (table instanceof HTableInterface) {
            ((HTableInterface) table).put(put);
        } else if (table instanceof HRegion) {
            ((HRegion) table).put(put, durable);
        } else {
            throw new RuntimeException("Cannot write to table of type " + table.getClass().getName());
        }
    }

    @Override
    public void write(HTableInterface table, List puts) throws IOException {
        table.put(puts);
    }

    @Override
    public boolean checkAndPut(HTableInterface table, byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException {
        return table.checkAndPut(put.getRow(), family, qualifier, expectedValue, put);
    }

    @Override
    public RowLock lockRow(HTableInterface table, byte[] rowKey) throws IOException {
        return table.lockRow(rowKey);
    }

    @Override
    public Integer lockRow(HRegion region, byte[] rowKey) throws IOException {
        return region.obtainRowLock(rowKey);
    }

    @Override
    public void unLockRow(HTableInterface table, RowLock lock) throws IOException {
        table.unlockRow(lock);
    }

    @Override
    public void unLockRow(HRegion region, Integer lock) {
        region.releaseRowLock(lock);
    }

}
