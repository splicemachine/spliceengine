package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.Externalizable;

/**
 * Created by dgomezferro on 4/4/14.
 */
public abstract class SpliceFunction2<Op extends SpliceOperation, From, From2, To>
    extends AbstractSpliceFunction<Op>
		implements ZipperFunction<From, From2, To>, Externalizable {

	public SpliceFunction2() {
	}

	protected SpliceFunction2(OperationContext<Op> operationContext) {
        super(operationContext);
	}
}
