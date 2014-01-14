package com.splicemachine.si.impl;

import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.hbase.IHTable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * A class that delegates all the interface methods. This is intended to be sub-classed to provide a "decorator" for the
 * interface. By having this class the decorator can only override the methods that need to be customized.
 */
public class STableReaderDelegate<Table, Result, Get, Scan, KeyValue, Scanner, Data> implements STableReader<Table, Result, Get, Scan, KeyValue, Scanner, Data>{
    STableReader<Table, Result, Get, Scan, KeyValue, Scanner, Data> delegate;

    public STableReaderDelegate(STableReader<Table, Result, Get, Scan, KeyValue, Scanner, Data> delegate) {
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
    public Iterator<Result> scan(Table table, Scan scan) throws IOException {
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
