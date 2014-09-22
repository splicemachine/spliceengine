package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.stats.TaskStats;

/**
 * Instances are invoked by SinkTask to perform the actual operation-specific Sinking logic.
 */
public interface OperationSink {

    TaskStats sink(SpliceRuntimeContext spliceRuntimeContext) throws Exception;

}
