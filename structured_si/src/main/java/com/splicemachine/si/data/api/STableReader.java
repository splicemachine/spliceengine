package com.splicemachine.si.data.api;

import java.io.IOException;
import java.util.Iterator;

/**
 * Means of opening data and reading data from them.
 */
public interface STableReader<Result> {
    STable open(String tableName) throws IOException;
    void close(STable table) throws IOException;

    Result get(STable table, SGet get) throws IOException;
    Iterator<Result> scan(STable table, SScan scan) throws IOException;
}
