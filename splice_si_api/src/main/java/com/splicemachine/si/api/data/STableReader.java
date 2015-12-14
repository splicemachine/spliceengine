package com.splicemachine.si.api.data;

import com.splicemachine.collections.CloseableIterator;

import java.io.IOException;

/**
 * Means of opening tables and reading data from them. This is an abstraction over the execution of HBase read operations.
 */
public interface STableReader<Table, Get, Scan,Result> {
    Table open(String tableName) throws IOException;
    void close(Table table) throws IOException;
    String getTableName(Table table);
    Result get(Table table, Get get) throws IOException;
    CloseableIterator<Result> scan(Table table, Scan scan) throws IOException;
    void openOperation(Table table) throws IOException;
    void closeOperation(Table table) throws IOException;
}
