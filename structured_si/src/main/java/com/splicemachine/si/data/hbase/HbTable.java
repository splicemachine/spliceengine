package com.splicemachine.si.data.hbase;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Wrapper that makes an HBase table comply with an interface that allows regions and tables to be used in a uniform manner.
 */
public class HbTable implements IHTable {
    final HTableInterface table;

    public HbTable(HTableInterface table) {
        this.table = table;
    }

    @Override
    public String getName() {
        return Bytes.toString(table.getTableName());
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
    public Iterator<Result> scan(Scan scan) throws IOException {
        if(scan.getStartRow() == null) {
            scan.setStartRow(new byte[]{});
        }
        final ResultScanner scanner = table.getScanner(scan);
        return scanner.iterator();
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
    public void put(Put put) throws IOException {
        table.put(put);
    }

    @Override
    public void put(Put put, Integer rowLock) throws IOException {
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
    public OperationStatus[] batchPut(Pair<Mutation, Integer>[] puts) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean checkAndPut(byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException {
        return table.checkAndPut(put.getRow(), family, qualifier, expectedValue, put);
    }

    @Override
    public void delete(Delete delete, Integer rowLock) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public Integer lockRow(byte[] rowKey) throws IOException {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void unLockRow(Integer lock) throws IOException {
        throw new RuntimeException("not implemented");
    }
}
