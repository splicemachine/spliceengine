package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ProjectRestrictOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.StreamLogUtils;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;

/**
 * Created by jleach on 5/1/15.
 */
public class ProjectRestrictFlatMapFunction<Op extends SpliceOperation> extends SpliceFlatMapFunction<Op,LocatedRow,LocatedRow> {
    protected boolean initialized;
    protected ProjectRestrictOperation op;
    protected ExecutionFactory executionFactory;

    public ProjectRestrictFlatMapFunction() {
        super();
    }

    public ProjectRestrictFlatMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }

    @Override
    public Iterable<LocatedRow> call(LocatedRow from) throws Exception {
        if (!initialized) {
            initialized = true;
            op = (ProjectRestrictOperation) getOperation();
            executionFactory = op.getExecutionFactory();
        }
        op.setCurrentRow(from.getRow());
        op.source.setCurrentRow(from.getRow());
        if (!op.getRestriction().apply(from.getRow())) {
            operationContext.recordFilter();
            StreamLogUtils.logOperationRecordWithMessage(from,operationContext,"filtered");
            return Collections.emptyList();
        }
//        ExecRow execRow = executionFactory.getValueRow(numberOfColumns);
        ExecRow preCopy = op.doProjection(from.getRow());
        LocatedRow locatedRow = new LocatedRow(from.getRowLocation(), preCopy);
        op.setCurrentLocatedRow(locatedRow);
        StreamLogUtils.logOperationRecord(locatedRow,operationContext);
        return Collections.singletonList(locatedRow);
    }
}
