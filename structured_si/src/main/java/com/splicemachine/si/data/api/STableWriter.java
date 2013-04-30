package com.splicemachine.si.data.api;

import java.io.IOException;
import java.util.List;

/**
 * Means of writing to data. To be used in conjunction with STableReader.
 */
public interface STableWriter {
    void write(STable table, Object put) throws IOException;
    void write(STable table, Object put, SRowLock rowLock) throws IOException;
    void write(STable table, Object put, boolean durable) throws IOException;
    void write(STable table, List puts) throws IOException;

    SRowLock lockRow(STable table, Object rowKey) throws IOException;
    void unLockRow(STable table, SRowLock lock) throws IOException;
    boolean checkAndPut(STable table, Object family, Object qualifier, Object expectedValue, Object put) throws IOException;
}
