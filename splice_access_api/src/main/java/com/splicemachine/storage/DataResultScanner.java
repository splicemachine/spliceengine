package com.splicemachine.storage;

import com.splicemachine.metrics.TimeView;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/15/15
 */
public interface DataResultScanner extends AutoCloseable{

    DataResult next() throws IOException;

    TimeView getReadTime();

    long getBytesOutput();

    long getRowsFiltered();

    long getRowsVisited();

    @Override void close() throws IOException;
}
