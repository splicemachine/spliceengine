package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import java.io.Externalizable;

/**
 * @author Scott Fines
 *         Created on: 11/4/13
 */
public interface AggregateContext extends Externalizable {

    void init(SpliceOperationContext context) throws StandardException;

    SpliceGenericAggregator[] getAggregators() throws StandardException;

    ExecIndexRow getSortTemplateRow() throws StandardException;

    ExecIndexRow getSourceIndexRow();

    SpliceGenericAggregator[] getDistinctAggregators();

    SpliceGenericAggregator[] getNonDistinctAggregators();
}
