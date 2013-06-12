package com.splicemachine.si.data.api;

import java.io.IOException;
import java.util.Iterator;

/**
 * Means of opening tables and reading data from them.
 */
public interface STableReader<Table extends STable, Result, Get, Scan> {
    Table open(String tableName) throws IOException;
    void close(Table table) throws IOException;

    Result get(Table table, Get get) throws IOException;
    Iterator<Result> scan(Table table, Scan scan) throws IOException;
}
