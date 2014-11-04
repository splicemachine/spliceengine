package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportOperation;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportOperationSink;
import com.splicemachine.si.api.TxnView;

import java.io.IOException;

/**
 * Builds a OperationSink (SinkTask delegate) given a SinkingOperation.
 */
public class OperationSinkFactory {

    public static OperationSink create(SinkingOperation operation,
                                       byte[] taskId,
                                       TxnView txn,
                                       long statementId,
                                       long waitTimeNs) throws IOException {

        //
        // ExportOperationSink. Does not have a destination table.
        //
        if (operation instanceof ExportOperation) {
            return new ExportOperationSink((ExportOperation) operation, taskId);
        }

        //
        // TableOperationSink, writes to temp table or operation-specific destination table (DML).
        //
        byte[] destinationTable = getDestinationTableBytes(operation);
        return new TableOperationSink(taskId, operation, SpliceDriver.driver().getTableWriter(), txn, statementId, waitTimeNs, destinationTable);
    }

    private static byte[] getDestinationTableBytes(SinkingOperation operation) {
        if (operation instanceof DMLWriteOperation) {
            return ((DMLWriteOperation) operation).getDestinationTable();
        }
        return SpliceDriver.driver().getTempTable().getTempTableName();
    }

}
