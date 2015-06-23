package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.OperationSink;
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
        throw new IOException("Should not run...");
    }

    private static byte[] getDestinationTableBytes(SinkingOperation operation) {
        if (operation instanceof DMLWriteOperation) {
            return ((DMLWriteOperation) operation).getDestinationTable();
        }
        return SpliceDriver.driver().getTempTable().getTempTableName();
    }

}
