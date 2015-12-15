package com.splicemachine.storage;

import com.splicemachine.metrics.TimeView;
import com.splicemachine.storage.DataCell;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 12/14/15
 */
public interface DataScanner extends AutoCloseable{

    List<DataCell> next(int limit) throws IOException;

    TimeView getReadTime();

    long getBytesOutput();

    long getRowsFiltered();

    long getRowsVisited();

    @Override void close() throws IOException;
}
