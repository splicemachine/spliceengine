package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.Externalizable;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.WarningCollector;

/**
 * @author  jyuan on 7/25/14.
 */
public interface WindowContext extends WarningCollector,Externalizable {

    void init(SpliceOperationContext context) throws StandardException;

    /**
     * The list of window functions that will be executed on the columns
     * of each row.<br/>
     * If there is more than one <code>WindowAggregator</code> int this
     * list, it is because they all share identical <code>over()</code>
     * clauses.  They were batched up this way so that they can all be
     * applied to the same <code>ExecRow</code>.
     * @return the list of window functions to be applied to a given row.
     */
    WindowAggregator[] getWindowFunctions();

    ExecRow getSortTemplateRow() throws StandardException;

    ExecRow getSourceIndexRow();

    /**
     * All aggregators in this list of window functions will
     * use the same key columns.
     * @return the key column array for all functions in this collection.
     */
    int[] getKeyColumns();

    /**
     * All aggregators in this list of window functions will
     * use the same key orders.
     * @return the key orders array for all functions in this collection.
     */
    boolean[] getKeyOrders();

    /**
     * All aggregators in this list of window functions will
     * use the same partition.
     * @return the partition array for all functions in this collection.
     */
    int[] getPartitionColumns();

    FrameDefinition getFrameDefinition();

    int[] getSortColumns();
}
