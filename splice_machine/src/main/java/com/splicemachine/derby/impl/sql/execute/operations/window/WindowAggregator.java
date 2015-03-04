package com.splicemachine.derby.impl.sql.execute.operations.window;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

/**
 * @author Jeff Cunningham
 *         Date: 9/15/14
 */
public interface WindowAggregator {
    void accumulate(ExecRow nextRow, ExecRow accumulatorRow) throws StandardException;

    void finish(ExecRow row) throws StandardException;

    boolean initialize(ExecRow row) throws StandardException;

    int getResultColumnId();

    int getFunctionColumnId();

    int[] getPartitionColumns();

    int[] getKeyColumns();

    int[] getSortColumns();

    boolean[] getKeyOrders();

    FrameDefinition getFrameDefinition();

    String getName();
}
