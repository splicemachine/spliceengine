package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.JoinOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

import javax.annotation.Nullable;
import java.io.Serializable;

/**
 *
 */
public abstract class SpliceJoinFunction<Op extends SpliceOperation, From, To>
    extends SpliceFunction<Op,From,To> implements Serializable {
    private static final long serialVersionUID = 3988079974858059941L;
    protected JoinOperation op = null;
    protected boolean initialized = false;
    protected int numberOfColumns = 0;
    protected ExecutionFactory executionFactory;
	public SpliceJoinFunction() {
        super();
	}
	public SpliceJoinFunction(OperationContext<Op> operationContext) {
        super(operationContext);
	}

    @Nullable
    @Override
    public To apply(@Nullable From from) {
        return super.apply(from);
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