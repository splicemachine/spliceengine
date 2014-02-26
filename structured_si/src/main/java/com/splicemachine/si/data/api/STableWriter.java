package com.splicemachine.si.data.api;

import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;

/**
 * Means of writing to Tables. To be used in conjunction with STableReader and SDataLib. This is an abstraction over the
 * HBase write operations.
 */
public interface STableWriter<Table, Mutation, Put, Delete> {
    void write(Table Table, Put put) throws IOException;
    void write(Table Table, Put put, Integer rowLock) throws IOException;
    void write(Table Table, Put put, boolean durable) throws IOException;
    void write(Table Table, List<Put> puts) throws IOException;
    OperationStatus[] writeBatch(Table table, Pair<Mutation, Integer>[] puts) throws IOException;

    void delete(Table Table, Delete delete, Integer lock) throws IOException;

    Integer lockRow(Table Table, byte[] rowKey) throws IOException;
    void unLockRow(Table Table, Integer lock) throws IOException;
    boolean checkAndPut(Table Table, byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException;
}
