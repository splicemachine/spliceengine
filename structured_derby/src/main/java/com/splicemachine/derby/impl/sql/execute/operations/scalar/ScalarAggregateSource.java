package com.splicemachine.derby.impl.sql.execute.operations.scalar;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 10/8/13
 */
public interface ScalarAggregateSource {
    ExecIndexRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException,IOException;
}
