package com.splicemachine.storage;

import java.io.IOException;

/**
 * Abstract representation of a Filter.
 *
 * @author Scott Fines
 *         Date: 12/16/15
 */
public interface DataFilter{

    enum ReturnCode{
        NEXT_ROW,INCLUDE,INCLUDE_AND_NEXT_COL,NEXT_COL,SEEK,SKIP
    }

    DataFilter.ReturnCode filterKeyValue(DataCell keyValue) throws IOException;

    boolean filterRow();

    void reset();
}
