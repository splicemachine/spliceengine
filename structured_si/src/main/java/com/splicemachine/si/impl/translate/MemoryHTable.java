package com.splicemachine.si.impl.translate;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.light.LGet;
import com.splicemachine.si.data.light.LStore;
import com.splicemachine.si.data.light.LTable;

public class MemoryHTable implements HTableInterface {
    private final HTableInterface delegate;
    private final Translator translator;
    private final STableReader reader;
    private final String tableName;
    private final LStore memoryStore;

    public MemoryHTable(HTableInterface delegate, Translator translator, STableReader reader, String tableName, LStore memoryStore) {
        this.delegate = delegate;
        this.translator = translator;
        this.reader = reader;
        this.tableName = tableName;
        this.memoryStore = memoryStore;
    }

    @Override
    public byte[] getTableName() {
        return delegate.getTableName();
    }

    @Override
    public TableName getName() {
        return delegate.getName();
    }

    @Override
    public Configuration getConfiguration() {
        return delegate.getConfiguration();
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        return delegate.getTableDescriptor();
    }

    @Override
    public boolean exists(Get get) throws IOException {
        return delegate.exists(get);
    }

    @Override
    public Boolean[] exists(List<Get> gets) throws IOException {
        return delegate.exists(gets);
    }

    @Override
    public void batch(List<? extends Row> actions, Object[] results) throws IOException, InterruptedException {
        delegate.batch(actions, results);
    }

    @Override
    public Object[] batch(List<? extends Row> actions) throws IOException, InterruptedException {
        return delegate.batch(actions);
    }

    @Override
    public <R> void batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback) throws
            IOException, InterruptedException {
        delegate.batchCallback(actions, results, callback);
    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> actions, Batch.Callback<R> callback) throws IOException,
            InterruptedException {
        return delegate.batchCallback(actions, callback);
    }

    @Override
    public Result get(Get get) throws IOException {
        final LTable memoryTable = memoryStore.open(tableName);
        try {
            final Object translatedGet = translator.translate(get);
            return (Result) translator.translateResult(memoryStore.get(memoryTable, (LGet) translatedGet));
        } finally {
            memoryStore.close(memoryTable);
        }
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
        return delegate.get(gets);
    }

    @Override
    public Result getRowOrBefore(byte[] row, byte[] family) throws IOException {
        return delegate.getRowOrBefore(row, family);
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        return delegate.getScanner(scan);
    }

    @Override
    public ResultScanner getScanner(byte[] family) throws IOException {
        return delegate.getScanner(family);
    }

    @Override
    public ResultScanner getScanner(byte[] family, byte[] qualifier) throws IOException {
        return delegate.getScanner(family, qualifier);
    }

    @Override
    public void put(Put put) throws IOException {
        delegate.put(put);
    }

    @Override
    public void put(List<Put> puts) throws IOException {
        delegate.put(puts);
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) throws IOException {
        return delegate.checkAndPut(row, family, qualifier, value, put);
    }

    @Override
    public void delete(Delete delete) throws IOException {
        delegate.delete(delete);
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
        delegate.delete(deletes);
    }

    @Override
    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) throws IOException {
        return delegate.checkAndDelete(row, family, qualifier, value, delete);
    }

    @Override
    public void mutateRow(RowMutations rm) throws IOException {
        delegate.mutateRow(rm);
    }

    @Override
    public Result append(Append append) throws IOException {
        return delegate.append(append);
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        return delegate.increment(increment);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) throws IOException {
        return delegate.incrementColumnValue(row, family, qualifier, amount);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability) throws IOException {
        return delegate.incrementColumnValue(row, family, qualifier, amount, durability);
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
        return delegate.incrementColumnValue(row, family, qualifier, amount, writeToWAL);
    }

    @Override
    public boolean isAutoFlush() {
        return delegate.isAutoFlush();
    }

    @Override
    public void flushCommits() throws IOException {
        delegate.flushCommits();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] row) {
        return delegate.coprocessorService(row);
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable) throws ServiceException, Throwable {
        return delegate.coprocessorService(service, startKey, endKey, callable);
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T, R> callable, Batch.Callback<R> callback) throws ServiceException, Throwable {
        delegate.coprocessorService(service, startKey, endKey, callable, callback);
    }

    @Override
    public void setAutoFlush(boolean autoFlush) {
        delegate.setAutoFlush(autoFlush);
    }

    @Override
    public void setAutoFlush(boolean autoFlush, boolean clearBufferOnFail) {
        delegate.setAutoFlush(autoFlush, clearBufferOnFail);
    }

    @Override
    public void setAutoFlushTo(boolean autoFlush) {
        delegate.setAutoFlushTo(autoFlush);
    }

    @Override
    public long getWriteBufferSize() {
        return delegate.getWriteBufferSize();
    }

    @Override
    public void setWriteBufferSize(long writeBufferSize) throws IOException {
        delegate.setWriteBufferSize(writeBufferSize);
    }
}
