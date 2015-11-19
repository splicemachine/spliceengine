package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

import org.apache.spark.api.java.function.Function2;

import java.io.Externalizable;

/**
 * Created by dgomezferro on 4/4/14.
 */
public abstract class SpliceFunction2<Op extends SpliceOperation, From, From2, To>
    extends AbstractSpliceFunction<Op>
		implements Function2<From, From2, To>, Externalizable {

	public SpliceFunction2() {
	}

	protected SpliceFunction2(OperationContext<Op> operationContext) {
        super(operationContext);
	}

    public String getSparkName() {
        return this.getClass().getSimpleName();
        // return this.getClass().getSimpleName().replace("Function", "");
    }

}