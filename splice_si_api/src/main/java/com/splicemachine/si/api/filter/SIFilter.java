package com.splicemachine.si.api.filter;

import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataFilter;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/9/14
 */
public interface SIFilter{

    /**
     * Reset the filter for the next row.
     */
    void nextRow();

    /**
     * @return the accumulator used in the filter
     */
    RowAccumulator getAccumulator();

    DataFilter.ReturnCode filterCell(DataCell kv) throws IOException;
}
