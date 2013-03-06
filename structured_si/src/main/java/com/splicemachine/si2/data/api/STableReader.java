package com.splicemachine.si2.data.api;

import java.util.Iterator;

/**
 * Means of opening data and reading data from them.
 */
public interface STableReader {
    STable open(String relationIdentifier);
    void close(STable table);

    Object get(STable table, SGet get);
    Iterator scan(STable table, SScan scan);
}
