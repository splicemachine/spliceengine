package com.splicemachine.si2.data.api;

import java.util.List;

/**
 * Means of writing to data. To be used in conjunction with STableReader.
 */
public interface STableWriter {
    void write(STable table, Object put);
    void write(STable table, Object put, SRowLock rowLock);
    void write(STable table, Object put, boolean durable);
    void write(STable table, List puts);

    boolean checkAndPut(STable table, Object family, Object qualifier, Object value, Object put);

    SRowLock lockRow(STable table, Object rowKey);
    void unLockRow(STable table, SRowLock lock);
}
