package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.sparkproject.guava.common.base.Function;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Created by dgomezferro on 4/4/14.
 */
public abstract class SpliceJoinFlatMapFunction<Op extends SpliceOperation, From, To>
		extends SpliceFlatMapFunction<Op,From,To> implements Serializable {
    public JoinOperation op = null;
    public boolean initialized = false;
    public int numberOfColumns = 0;
    public ExecutionFactory executionFactory;

    public SpliceJoinFlatMapFunction() {
	}

	public SpliceJoinFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
	}

    protected void checkInit () {
        if (!initialized) {
            op = (JoinOperation) getOperation();
            numberOfColumns = op.getLeftNumCols()+op.getRightNumCols();
            executionFactory = op.getExecutionFactory();
            initialized = true;
        }
    }
}