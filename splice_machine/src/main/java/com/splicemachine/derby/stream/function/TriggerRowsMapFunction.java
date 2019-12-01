/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.TriggerExecutionContext;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Extract the desired columns out of the trigger result set.
 *
 */
public class TriggerRowsMapFunction<Op extends SpliceOperation> extends SpliceFunction<Op,ExecRow,ExecRow> {
    protected boolean initialized;
    protected TriggerExecutionContext tec;
    boolean isOld;
    Op operation;
    ExecRow execRowDefinition;
    int nCols;

    public TriggerRowsMapFunction() {
        super();
    }

    public TriggerRowsMapFunction(OperationContext<Op> operationContext, boolean isOld) {
        super(operationContext);
        this.isOld = isOld;
    }


    @Override
    public ExecRow call(ExecRow from) throws Exception {
        if (!initialized) {
            initialized = true;
            tec = operationContext.getActivation().getLanguageConnectionContext().getTriggerExecutionContext();
            operation = operationContext.getOperation();
            execRowDefinition = operation.getExecRowDefinition();
            nCols = execRowDefinition.nColumns();
        }
        ExecRow row;
        if (isOld)
            row = tec.buildOldRow(from, true);
        else
            row = tec.buildNewRow(from, true);

        if (row.nColumns() > nCols) {
            ExecRow result = new ValueRow(nCols);
            for (int i = 1; i <= nCols; i++)
            result.setColumn(i, row.getColumn(i));
            row = result;
        }
        operation.setCurrentRow(row);
        return row;
    }

    @Override
    public ExecRow getExecRow() throws StandardException {
        return operationContext.getOperation().getSubOperations().get(0).getExecRowDefinition();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(isOld);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        isOld = in.readBoolean();
    }
}
