package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.NormalizeOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 11/4/15.
 */
public class NormalizeFunction extends SpliceFunction<NormalizeOperation, LocatedRow, LocatedRow> {
    private static final long serialVersionUID = 7780564699906451370L;

    public NormalizeFunction() {
    }

    public NormalizeFunction(OperationContext<NormalizeOperation> operationContext) {
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
    public LocatedRow call(LocatedRow sourceRow) throws Exception {

        NormalizeOperation normalize = operationContext.getOperation();
        normalize.source.setCurrentRow(sourceRow.getRow());
        ExecRow normalized = null;
        if (sourceRow != null) {
            normalized = normalize.normalizeRow(sourceRow.getRow(), true);
        }
        getActivation().setCurrentRow(normalized, normalize.getResultSetNumber());
        return new LocatedRow(sourceRow.getRowLocation(), normalized.getClone());
    }


}
