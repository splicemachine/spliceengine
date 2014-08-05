package com.splicemachine.derby.impl.sql.execute.operations.window;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.WarningCollector;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import java.io.Externalizable;

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

    WindowFrame getWindowFrame();
}
