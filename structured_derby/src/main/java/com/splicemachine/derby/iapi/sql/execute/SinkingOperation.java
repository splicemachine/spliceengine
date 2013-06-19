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
 * before computing result rows.
 */
public interface SinkingOperation {

    /**
     * Get next ExecRow to sink to an intermediate table as prep for computing result rows
     */
    ExecRow getNextSinkRow() throws StandardException;

    /**
     * @return a function converting non-null ExecRow objects into Put objects.
     */
    public OperationSink.Translator getTranslator() throws IOException;

}
