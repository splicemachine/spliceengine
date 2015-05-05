package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;

/**
 * Created by jleach on 5/4/15.
 */
public interface IterableJoinFunction {
    public boolean hasNext();
    public ExecRow getRightRow();
    public ExecRow getLeftRow();
    public RowLocation getLeftRowLocation();
    public boolean wasRightOuterJoin();
    public ExecutionFactory getExecutionFactory();
    public int getNumberOfColumns();
    public void setCurrentLocatedRow(LocatedRow locatedRow);
    public int getResultSetNumber();

}
