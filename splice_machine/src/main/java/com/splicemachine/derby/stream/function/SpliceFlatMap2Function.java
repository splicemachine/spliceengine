package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.OperationContext;
import com.splicemachine.derby.stream.function.AbstractSpliceFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;

import java.io.Externalizable;

/**
 * Created by dgomezferro on 4/4/14.
 */
public abstract class SpliceFlatMap2Function<Op extends SpliceOperation, From, From2, To>
    extends AbstractSpliceFunction<Op>
		implements FlatMapFunction2<From, From2, To>, Externalizable {
   	public SpliceFlatMap2Function() {
	}

	protected SpliceFlatMap2Function(OperationContext<Op> operationContext) {
        super(operationContext);
	}

}