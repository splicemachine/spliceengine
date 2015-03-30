package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.scalar.ScalarAggregateSource;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 10/8/13
 */
public class OperationScalarAggregateSource implements ScalarAggregateSource {
    private final SpliceOperation source;
    private final ExecIndexRow sourceExecIndexRow;
    private final boolean doClone;

    public OperationScalarAggregateSource(SpliceOperation source, ExecIndexRow sourceExecIndexRow, boolean doClone) {
        this.source = source;
        this.sourceExecIndexRow = sourceExecIndexRow;
        this.doClone = doClone;
    }

    @Override
    public ExecIndexRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        ExecRow nextRow = source.nextRow(spliceRuntimeContext);
        if(nextRow!=null){
            sourceExecIndexRow.execRowToExecIndexRow(doClone?nextRow.getClone():nextRow);
            return sourceExecIndexRow;
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        //
    }
}
