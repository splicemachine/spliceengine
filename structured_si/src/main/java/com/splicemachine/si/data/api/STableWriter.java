package com.splicemachine.si.data.api;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Means of writing to Tables. To be used in conjunction with STableReader and SDataLib. This is an abstraction over the
 * HBase write operations.
 */
public interface STableWriter<Table, Mutation, Put, Delete> {
    void write(Table Table, Put put) throws IOException;
    void write(Table Table, Put put, HRegion.RowLock rowLock) throws IOException;
    void write(Table Table, Put put, boolean durable) throws IOException;
    void write(Table Table, List<Put> puts) throws IOException;
    OperationStatus[] writeBatch(Table table, Pair<Mutation, HRegion.RowLock>[] puts) throws IOException;

    void delete(Table Table, Delete delete, HRegion.RowLock lock) throws IOException;

    HRegion.RowLock lockRow(Table Table, byte[] rowKey) throws IOException;
    void unLockRow(Table Table, HRegion.RowLock lock) throws IOException;
    boolean checkAndPut(Table Table, byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException;
}
