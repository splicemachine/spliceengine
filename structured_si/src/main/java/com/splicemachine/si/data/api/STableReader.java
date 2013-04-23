package com.splicemachine.si.data.api;

import java.io.IOException;
import java.util.Iterator;

/**
 * Means of opening data and reading data from them.
 */
public interface STableReader {
    STable open(String tableName) throws IOException;
    void close(STable table) throws IOException;

    Object get(STable table, SGet get) throws IOException;
    Iterator scan(STable table, SScan scan) throws IOException;
}
