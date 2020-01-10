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

/**
 *
 *
 *
 *
 *
 */
public class ProjectRestrictMapFunction<Op extends SpliceOperation> extends SpliceFunction<Op,ExecRow,ExecRow> {
    protected boolean initialized;
    protected ProjectRestrictOperation op;
    protected ExecutionFactory executionFactory;
    private   String [] expressions = null;

    public ProjectRestrictMapFunction() {
        super();
    }

    public ProjectRestrictMapFunction(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    public ProjectRestrictMapFunction(OperationContext<Op> operationContext, String[] expressions) {
        super(operationContext);
        this.expressions = expressions;
    }

    @Override
    public ExecRow call(ExecRow from) throws Exception {
        if (!initialized) {
            initialized = true;
            op = (ProjectRestrictOperation) getOperation();
            executionFactory = op.getExecutionFactory();
        }
        op.setCurrentRow(from);
        op.source.setCurrentRow(from);
        ExecRow preCopy = op.doProjection(from);
        preCopy.setKey(from.getKey());
        op.setCurrentRow(preCopy);
        return preCopy;
    }

    @Override
    public ExecRow getExecRow() throws StandardException {
        return operationContext.getOperation().getSubOperations().get(0).getExecRowDefinition();
    }

    @Override
    public boolean hasNativeSparkImplementation() {
        ProjectRestrictOperation op = (ProjectRestrictOperation) operationContext.getOperation();
        if (op.projection != null) {
            if (!op.hasExpressions())
                return false;
        }
        return true;
    }

    @Override
    public Pair<Dataset<Row>, OperationContext> nativeTransformation(Dataset<Row> input, OperationContext context) {
        ProjectRestrictOperation op = (ProjectRestrictOperation) operationContext.getOperation();
        Dataset<Row> df = null;
        // TODO:  Enable the commented try-catch block after regression testing.
        //        This would be a safeguard against unanticipated exceptions:
        //             org.apache.spark.sql.catalyst.parser.ParseException
        //             org.apache.spark.sql.AnalysisException
        //    ... which may occur if the Splice parser fails to detect a
        //        SQL expression which SparkSQL does not support.
        if (op.hasExpressions()) {
//      try {
            df = input.selectExpr(op.getExpressions());
            return Pair.newPair(df, context);
//        }
//        catch (Exception e) {
//        }
        }
        int[] mapping = op.projectMapping;
        Column[] columns = new Column[mapping.length];
        for (int i = 0; i < mapping.length; ++i) {
            columns[i] = input.col("c" + (mapping[i] - 1));
        }
        df = input.select(columns);
        return Pair.newPair(df, context);
    }
}
