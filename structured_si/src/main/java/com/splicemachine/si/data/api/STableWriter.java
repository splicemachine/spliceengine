package com.splicemachine.si.data.api;

import java.io.IOException;
import java.util.List;

/**
 * Means of writing to data. To be used in conjunction with STableReader.
 */
public interface STableWriter<Put, Delete, Data> {
    void write(STable table, Put put) throws IOException;
    void write(STable table, Put put, SRowLock rowLock) throws IOException;
    void write(STable table, Put put, boolean durable) throws IOException;
    void write(STable table, List<Put> puts) throws IOException;

    void delete(STable table, Delete delete, SRowLock lock) throws IOException;

    SRowLock lockRow(STable table, Data rowKey) throws IOException;
    void unLockRow(STable table, SRowLock lock) throws IOException;
    boolean checkAndPut(STable table, Data family, Data qualifier, Data expectedValue, Put put) throws IOException;
}
