package com.splicemachine.si.data.api;

import com.splicemachine.utils.CloseableIterator;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Iterator;

/**
 * Means of opening tables and reading data from them. This is an abstraction over the execution of HBase read operations.
 */
public interface STableReader<Table, Get, Scan> {
    Table open(String tableName) throws IOException;
    void close(Table table) throws IOException;
    String getTableName(Table table);

    Result get(Table table, Get get) throws IOException;
    CloseableIterator<Result> scan(Table table, Scan scan) throws IOException;

    // These methods deal with low-level, server-side region scanners.

    void openOperation(Table table) throws IOException;
    void closeOperation(Table table) throws IOException;
}
