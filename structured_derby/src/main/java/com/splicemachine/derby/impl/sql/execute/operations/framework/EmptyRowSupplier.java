package com.splicemachine.derby.impl.sql.execute.operations.framework;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.AggregateContext;
import com.splicemachine.derby.utils.StandardSupplier;

public class EmptyRowSupplier implements StandardSupplier<ExecRow>{
	AggregateContext aggregateContext;
	public EmptyRowSupplier(AggregateContext aggregateContext) {
		this.aggregateContext = aggregateContext;
	}
        @Override
        public ExecRow get() throws StandardException {
            return aggregateContext.getSortTemplateRow();
        }
}