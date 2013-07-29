package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.derby.utils.marshall.RowEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

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

    String getTransactionID();

    RowEncoder getRowEncoder() throws StandardException;
}
