package com.splicemachine.si.data.api;

import java.io.IOException;
import java.util.List;

/**
 * Means of writing to Tables. To be used in conjunction with STableReader.
 */
public interface STableWriter<Table, Put, Delete, Data, Lock> {
    void write(Table Table, Put put) throws IOException;
    void write(Table Table, Put put, Lock rowLock) throws IOException;
    void write(Table Table, Put put, boolean durable) throws IOException;
    void write(Table Table, List<Put> puts) throws IOException;

    void delete(Table Table, Delete delete, Lock lock) throws IOException;

    Lock lockRow(Table Table, Data rowKey) throws IOException;
    void unLockRow(Table Table, Lock lock) throws IOException;
    boolean checkAndPut(Table Table, Data family, Data qualifier, Data expectedValue, Put put) throws IOException;
}
