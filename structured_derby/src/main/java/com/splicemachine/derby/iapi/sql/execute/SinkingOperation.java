package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.derby.impl.sql.execute.operations.OperationSink;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * User: pjt
 * Date: 6/18/13
 */

/**
 * Interface for SpliceOperations that need to sink rows from their children
 * before
 */
public interface SinkingOperation {

    /*
     * Get next row to sink to another table, as opposed to the computed relational row
     */
    ExecRow getNextSinkRow() throws StandardException;

    /**
     * Only needs to be implemented by parallel-type tasks (e.g. tasks which also implement sink()).
     *
     * @return a function converting non-null ExecRow objects into Put objects.
     */
    public OperationSink.Translator getTranslator() throws IOException;

}
