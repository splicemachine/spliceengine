package com.splicemachine.derby.impl.sql.execute.operations.iapi;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.stats.TaskStats;

/**
 * Instances are invoked by SinkTask to perform the actual operation-specific Sinking logic.
 */
public interface OperationSink {

    /**
     * A given instance's sink method is only called once.
     */
    TaskStats sink(SpliceRuntimeContext spliceRuntimeContext) throws Exception;

}
