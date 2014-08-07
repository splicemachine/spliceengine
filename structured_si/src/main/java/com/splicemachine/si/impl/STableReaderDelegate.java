package com.splicemachine.si.impl;

import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.si.data.api.STableReader;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

/**
 * A class that delegates all the interface methods. This is intended to be sub-classed to provide a "decorator" for the
 * interface. By having this class the decorator can only override the methods that need to be customized.
 */
public class STableReaderDelegate<Table, Get, Scan> implements STableReader<Table, Get, Scan>{
    STableReader<Table, Get, Scan> delegate;

    public STableReaderDelegate(STableReader<Table, Get, Scan> delegate) {
        this.delegate = delegate;
    }

    @Override
    public Table open(String tableName) throws IOException {
        return delegate.open(tableName);
    }

    @Override
    public void close(Table table) throws IOException {
        delegate.close(table);
    }

    @Override
    public String getTableName(Table table) {
        return delegate.getTableName(table);
    }

    @Override
    public Result get(Table table, Get get) throws IOException {
        return delegate.get(table, get);
    }

    @Override
    public CloseableIterator<Result> scan(Table table, Scan scan) throws IOException {
        return delegate.scan(table, scan);
    }

    @Override
    public void closeOperation(Table table) throws IOException {
        delegate.closeOperation(table);
    }

    @Override
    public void openOperation(Table table) throws IOException {
        delegate.openOperation(table);
    }
}
