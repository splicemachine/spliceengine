package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.iapi.DataSet;

/**
 * Created by jyuan on 3/14/17.
 */
public interface HBaseBulkImporter {
    DataSet<ExecRow> write() throws StandardException;
}
