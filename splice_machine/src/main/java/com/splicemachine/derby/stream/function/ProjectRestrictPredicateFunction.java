/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ProjectRestrictOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.Pair;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import javax.annotation.Nullable;

/**
 * Created by jleach on 5/1/15.
 */
public class ProjectRestrictPredicateFunction<Op extends SpliceOperation> extends SplicePredicateFunction<Op,ExecRow> {
    protected boolean initialized;
    protected ProjectRestrictOperation op;

    public ProjectRestrictPredicateFunction() {
        super();
    }

    public ProjectRestrictPredicateFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public boolean apply(@Nullable ExecRow from) {
        try {
            init();
            op.setCurrentRow(from);
            op.source.setCurrentRow(from);
            if (!op.getRestriction().apply(from)) {
                operationContext.recordFilter();
                return false;
            }
            return true;
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExecRow getExecRow() throws StandardException {
        init();
        return op.getSubOperations().get(0).getExecRowDefinition();
    }

    private void init() {
        if (!initialized) {
            initialized = true;
            op = (ProjectRestrictOperation) getOperation();
        }
    }

    @Override
    public boolean hasNativeSparkImplementation() {
        ProjectRestrictOperation op = (ProjectRestrictOperation) operationContext.getOperation();
        if (op.hasFilterPred()) {
            return true;
        }
        return false;
    }

    @Override
    public Pair<Dataset<Row>, OperationContext> nativeTransformation(Dataset<Row> input, OperationContext context) {
        ProjectRestrictOperation op = (ProjectRestrictOperation) operationContext.getOperation();
        Dataset<Row> df = input.where(op.getFilterPred());
        return Pair.newPair(df, context);
    }
}
