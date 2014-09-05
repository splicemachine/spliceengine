package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.Externalizable;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.WarningCollector;

/**
 * Created by jyuan on 7/25/14.
 */
public interface WindowContext extends WarningCollector,Externalizable {

    void init(SpliceOperationContext context) throws StandardException;

    int[] getPartitionColumns();

    int[] getSortColumns();

    boolean[] getSortOrders();

    int[] getKeyColumns();

    boolean[] getKeyOrders();

    FrameDefinition getFrameDefinition();

    WindowAggregator[] getWindowFunctions();

    ExecIndexRow getSortTemplateRow() throws StandardException;

    ExecIndexRow getSourceIndexRow();
}
