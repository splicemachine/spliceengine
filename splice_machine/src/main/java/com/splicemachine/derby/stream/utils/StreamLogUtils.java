package com.splicemachine.derby.stream.utils;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

/**
 *
 * Utils for logging a locatedRow
 *
 */
public class StreamLogUtils {
    private static Logger LOG = Logger.getLogger(StreamLogUtils.class);

    public static void logOperationRecord(LocatedRow locatedRow, OperationContext operationContext) {
        if (LOG.isTraceEnabled()) {
            SpliceOperation op = operationContext.getOperation();
            SpliceLogUtils.trace(LOG, "%s (%d) -> %s", op.getName(),op.resultSetNumber(), locatedRow);
        }
    }

    public static void logOperationRecord(LocatedRow locatedRow, SpliceOperation operation) {
        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG, "%s (%d) -> %s", operation.getName(),operation.resultSetNumber(), locatedRow);
        }
    }


    public static void logOperationRecordWithMessage(LocatedRow locatedRow, OperationContext operationContext, String message) {
        if (LOG.isTraceEnabled()) {
            SpliceOperation op = operationContext.getOperation();
            SpliceLogUtils.trace(LOG, "%s (%d) [%s] -> %s", op.getName(),op.resultSetNumber(), message, locatedRow);
        }
    }

}
