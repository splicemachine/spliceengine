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
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Strings;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;

/**
 * Created by yxia on 3/22/19.
 */
public class RecursiveUnionOperation extends UnionOperation {
    private static Logger LOG = Logger.getLogger(RecursiveUnionOperation.class);
    private DataSet rightDS;
    private final int MAX_LOOP = 20;
    protected static final String NAME = RecursiveUnionOperation.class.getSimpleName().replaceAll("Operation","");

    @Override
    public String getName() {
        return NAME;
    }

    public RecursiveUnionOperation() {
        super();
    }

    public RecursiveUnionOperation(SpliceOperation leftResultSet,
                          SpliceOperation rightResultSet,
                          Activation activation,
                          int resultSetNumber,
                          double optimizerEstimatedRowCount,
                          double optimizerEstimatedCost) throws StandardException{
        super(leftResultSet, rightResultSet, activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
//        init();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        leftResultSet = (SpliceOperation)in.readObject();
        rightResultSet = (SpliceOperation)in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(leftResultSet);
        out.writeObject(rightResultSet);
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return leftResultSet.getExecRowDefinition();
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return leftResultSet;
    }

    @Override
    public SpliceOperation getRightOperation() {
        return rightResultSet;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Arrays.asList(leftResultSet, rightResultSet);
    }

    @Override
    public String toString() {
        return "RecursiveUnionOperation{" +
                "left=" + leftResultSet +
                ", right=" + rightResultSet +
                '}';
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);

        return "RecursiveUnion:" + indent + "resultSetNumber:" + resultSetNumber
                + indent + "leftResultSet:" + leftResultSet
                + indent + "rightResultSet:" + rightResultSet;
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
        if(leftResultSet.isReferencingTable(tableNumber))
            return leftResultSet.getRootAccessedCols(tableNumber);
        else if(rightResultSet.isReferencingTable(tableNumber))
            return rightResultSet.getRootAccessedCols(tableNumber);

        return null;
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return leftResultSet.isReferencingTable(tableNumber) || rightResultSet.isReferencingTable(tableNumber);

    }

    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        OperationContext operationContext = dsp.createOperationContext(this);
        operationContext.pushScope();
        // The dataset can be read only once, so we have to return two datasets
        Pair<Pair<DataSet, DataSet>, Integer> right = leftResultSet.getDataSet(dsp).materialize2();
        DataSet resultDS = right.getFirst().getFirst();
        rightDS = right.getFirst().getSecond();

        right = rightResultSet.getDataSet(dsp).materialize2();

        int loop = 0;
        while (right.getSecond() > 0 && loop < MAX_LOOP) {
            loop ++;
            Pair<DataSet, Integer> result = resultDS.union(right.getFirst().getFirst(), operationContext).materialize();
            rightDS = right.getFirst().getSecond();

            resultDS = result.getFirst();

            right = rightResultSet.getDataSet(dsp).materialize2();
        }

        operationContext.popScope();
        return resultDS;
    }

    public DataSet getSelfReference() {
        return this.rightDS;
    }
}
