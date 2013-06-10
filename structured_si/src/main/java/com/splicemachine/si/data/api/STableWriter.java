package com.splicemachine.si.data.api;

import java.io.IOException;
import java.util.List;

/**
 * Means of writing to tables. To be used in conjunction with STableReader.
 */
public interface STableWriter<Table extends STable, Put, Delete, Data, Lock extends SRowLock> {
    void write(Table table, Put put) throws IOException;
    void write(Table table, Put put, Lock rowLock) throws IOException;
    void write(Table table, Put put, boolean durable) throws IOException;
    void write(Table table, List<Put> puts) throws IOException;

    void delete(Table table, Delete delete, Lock lock) throws IOException;

    Lock lockRow(Table table, Data rowKey) throws IOException;
    void unLockRow(Table table, Lock lock) throws IOException;
    boolean checkAndPut(Table table, Data family, Data qualifier, Data expectedValue, Put put) throws IOException;
}
