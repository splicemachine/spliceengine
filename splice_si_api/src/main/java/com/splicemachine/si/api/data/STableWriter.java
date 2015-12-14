package com.splicemachine.si.api.data;

import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.Pair;
import java.io.IOException;
import java.util.List;

/**
 * Means of writing to Tables. To be used in conjunction with STableReader and SDataLib. This is an abstraction over the
 * HBase write operations.
 */
public interface STableWriter<Delete,Mutation,OperationStatus,Put,RowLock,Table> {
    void write(Table Table, Put put) throws IOException;
    void write(Table Table, Put put, SRowLock rowLock) throws IOException;
    void write(Table Table, Put put, boolean durable) throws IOException;
    void write(Table Table, List<Put> puts) throws IOException;
    OperationStatus[] writeBatch(Table table, Pair[] puts) throws IOException;
    void delete(Table Table, Delete delete, SRowLock lock) throws IOException;
    /**
     * @param table the table to lock on
     * @param rowKey the row to lock
     * @return the id of the lock acquired, or {@code null} if the lock was not able to be acquired.
     */
    SRowLock tryLock(Table table, byte[] rowKey) throws IOException;

    SRowLock tryLock(Table table, ByteSlice rowKey) throws IOException;

    SRowLock lockRow(Table Table, byte[] rowKey) throws IOException;
    void unLockRow(Table Table, SRowLock lock) throws IOException;
    boolean checkAndPut(Table Table, byte[] family, byte[] qualifier, byte[] expectedValue, Put put) throws IOException;
    void increment(Table table,byte[] rowKey, byte[] family, byte[] qualifier, long amount) throws IOException;

}
