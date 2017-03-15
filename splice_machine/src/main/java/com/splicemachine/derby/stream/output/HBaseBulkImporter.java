package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;

/**
 * Created by jyuan on 3/14/17.
 */
public interface HBaseBulkImporter {
    DataSet<LocatedRow> write() throws StandardException;
}
