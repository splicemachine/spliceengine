package com.splicemachine.si.data.api;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Means of opening tables and reading data from them. This is an abstraction over the execution of HBase read operations.
 */
public interface STableReader<Table, Result, Get, Scan, KeyValue, Scanner, Data> {
    Table open(String tableName) throws IOException;
    void close(Table table) throws IOException;
    String getTableName(Table table);

    Result get(Table table, Get get) throws IOException;
    Iterator<Result> scan(Table table, Scan scan) throws IOException;

    // These methods deal with low-level, server-side region scanners.

    void openOperation(Table table) throws IOException;
    void closeOperation(Table table) throws IOException;
}
