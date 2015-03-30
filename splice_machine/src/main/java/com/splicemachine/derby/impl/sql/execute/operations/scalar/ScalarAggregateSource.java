package com.splicemachine.derby.impl.sql.execute.operations.scalar;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 10/8/13
 */
public interface ScalarAggregateSource {
    ExecIndexRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException,IOException;
    void close() throws IOException;
}
