package com.splicemachine.si.data.api;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.regionserver.OperationStatus;

/**
 * Means of writing to Tables. To be used in conjunction with STableReader and SDataLib. This is an abstraction over the
 * HBase write operations.
 */
public interface STableWriter<Table, Mutation, Put, Delete> {
    void write(Table Table, Put put) throws IOException;

    void write(Table Table, List<Put> puts) throws IOException;

    OperationStatus[] writeBatch(Table table, Mutation[] puts) throws IOException;

    void delete(Table Table, Delete delete) throws IOException;

    /**
     * @param table  the table to lock on
     * @param rowKey the row to lock
     * @return the id of the lock acquired, or {@code null} if the lock was not able to be acquired.
     */
    SRowLock tryLock(Table table, byte[] rowKey) throws IOException;

    //TODO -sf- can we replace this with an actual Lock abstraction?
    SRowLock lockRow(Table Table, byte[] rowKey) throws IOException;

    void unLockRow(Table Table, SRowLock lock) throws IOException;

    boolean checkAndPut(Table Table, byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException;
}
