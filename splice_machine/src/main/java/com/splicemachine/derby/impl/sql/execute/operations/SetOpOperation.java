/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.stream.iapi.OperationContext;
import org.spark_project.guava.base.Strings;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.compile.IntersectOrExceptNode;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;

/**
 *
 * Initial work on intersect and except set operations.  This
 * needs more work.
 *
 * XXX-TODO SPLICE-718
 *
 */
public class SetOpOperation extends SpliceBaseOperation {
    private static Logger LOG = Logger.getLogger(AnyOperation.class);
    protected static final String NAME = SetOpOperation.class.getSimpleName().replaceAll("Operation", "");

    @Override
    public String getName() {
        return NAME;
    }
    private SpliceOperation leftSource;
    private SpliceOperation rightSource;
    private int opType;
    private boolean all;
    private int rightDuplicateCount; // Number of duplicates of the current row from the right input
    private ExecRow leftInputRow;
    private ExecRow rightInputRow;
    private int[] intermediateOrderByColumns;
    private int[] intermediateOrderByDirection;
    private boolean[] intermediateOrderByNullsLow;

    public SetOpOperation() { }

    public SetOpOperation( SpliceOperation leftSource,
                           SpliceOperation rightSource,
                           Activation activation,
                           int resultSetNumber,
                           long optimizerEstimatedRowCount,
                           double optimizerEstimatedCost,
                           int opType,
                           boolean all,
                           int intermediateOrderByColumnsSavedObject,
                           int intermediateOrderByDirectionSavedObject,
                           int intermediateOrderByNullsLowSavedObject) throws StandardException {
        super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.leftSource = leftSource;
        this.rightSource = rightSource;
        this.leftInputRow = leftSource.getExecRowDefinition();
        this.rightInputRow = rightSource.getExecRowDefinition();
        this.resultSetNumber = resultSetNumber;
        this.opType = opType;
        this.all = all;

        ExecPreparedStatement eps = activation.getPreparedStatement();
        intermediateOrderByColumns = (int[]) eps.getSavedObject(intermediateOrderByColumnsSavedObject);
        intermediateOrderByDirection = (int[]) eps.getSavedObject(intermediateOrderByDirectionSavedObject);
        intermediateOrderByNullsLow = (boolean[]) eps.getSavedObject(intermediateOrderByNullsLowSavedObject);
        init();
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Arrays.asList(leftSource,rightSource);
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return leftSource;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(leftSource);
        out.writeObject(rightSource);
        out.writeInt(opType);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        leftSource = (SpliceOperation) in.readObject();
        rightSource = (SpliceOperation) in.readObject();
        opType = in.readInt();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        leftSource.init(context);
        rightSource.init(context);
        this.leftInputRow = leftSource.getExecRowDefinition();
        this.rightInputRow = rightSource.getExecRowDefinition();
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t", indentLevel);
        return new StringBuilder("Any:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("LeftSource:").append(leftSource.prettyPrint(indentLevel+1))
                .append(indent).append("RightSource:").append(rightSource.prettyPrint(indentLevel+1))
                .toString();
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return leftInputRow;
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public String toString() {
        return String.format("SetOpOperation {leftSource=%s,rightResult=%s,resultSetNumber=%d}",leftSource,rightSource,resultSetNumber);
    }

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        OperationContext operationContext = dsp.createOperationContext(this);
        if (this.opType==IntersectOrExceptNode.INTERSECT_OP) {
            return leftSource.getDataSet(dsp).intersect(
                    rightSource.getDataSet(dsp),
                    OperationContext.Scope.INTERSECT.displayName(),
                    operationContext,
                    true,
                    OperationContext.Scope.INTERSECT.displayName());
        }
        else if (this.opType==IntersectOrExceptNode.EXCEPT_OP) {
            return leftSource.getDataSet(dsp).subtract(
                    rightSource.getDataSet(dsp),
                    OperationContext.Scope.SUBTRACT.displayName(),
                    operationContext,
                    true,
                    OperationContext.Scope.SUBTRACT.displayName());
        } else {
            throw new RuntimeException("Operation Type not Supported "+opType);
        }

    }
}