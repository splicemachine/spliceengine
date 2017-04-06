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

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.function.KeyerFunction;
import com.splicemachine.derby.stream.function.MergeAllAggregatesFlatMapFunction;
import com.splicemachine.derby.stream.function.MergeNonDistinctAggregatesFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 *
 *
 * @author Scott Fines
 * Created on: 5/21/13
 */
public class DistinctScalarAggregateOperation extends GenericAggregateOperation {
    private static final long serialVersionUID = 1l;
    private int orderItem;
    private int[] keyColumns;
    private static final Logger LOG = Logger.getLogger(DistinctScalarAggregateOperation.class);
    protected static final String NAME = DistinctScalarAggregateOperation.class.getSimpleName().replaceAll("Operation", "");

    @Override
    public String getName() {
        return NAME;
    }

    @SuppressWarnings("UnusedDeclaration")
    public DistinctScalarAggregateOperation() {
    }

    @SuppressWarnings("UnusedParameters")
    public DistinctScalarAggregateOperation(SpliceOperation source,
                                            boolean isInSortedOrder,
                                            int aggregateItem,
                                            int orderItem,
                                            GeneratedMethod rowAllocator,
                                            int maxRowSize,
                                            int resultSetNumber,
                                            boolean singleInputRow,
                                            double optimizerEstimatedRowCount,
                                            double optimizerEstimatedCost) throws StandardException {
        super(source, aggregateItem, source.getActivation(), rowAllocator, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.orderItem = orderItem;
        init();
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        ExecRow clone = sourceExecIndexRow.getClone();
        // Set the default values to 0 in case a ProjectRestrictOperation has set the default values to 1.
        // That is done to avoid division by zero exceptions when executing a projection for defining the rows
        // before execution.
        EngineUtils.populateDefaultValues(clone.getRowArray(),0);
        return clone;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(orderItem);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        orderItem = in.readInt();
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        ExecPreparedStatement gsps = activation.getPreparedStatement();
        ColumnOrdering[] order =
                (ColumnOrdering[])
                        ((FormatableArrayHolder) gsps.getSavedObject(orderItem)).getArray(ColumnOrdering.class);
        keyColumns = new int[order.length];
        for (int index = 0; index < order.length; index++) {
            keyColumns[index] = order[index].getColumnId();
        }
    }

    @Override
    public String toString() {
        return String.format("DistinctScalarAggregateOperation {resultSetNumber=%d, source=%s}", resultSetNumber, source);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        OperationContext operationContext = dsp.createOperationContext(this);
        DataSet<LocatedRow> dataSet = source.getDataSet(dsp);
        DataSet<LocatedRow> ds2 = dataSet.keyBy(new KeyerFunction(operationContext, keyColumns), null, true, "Prepare Keys")
            .reduceByKey(new MergeNonDistinctAggregatesFunction(operationContext), false, true, "Reduce")
            .values(null, false, operationContext, true, "Read Values");
        DataSet<LocatedRow> ds3 = ds2.mapPartitions(new MergeAllAggregatesFlatMapFunction(operationContext, false), false, true, "First Aggregation");
        DataSet<LocatedRow> ds4 = ds3.coalesce(1, true, false, operationContext, true, "Coalesce");
        return ds4.mapPartitions(new MergeAllAggregatesFlatMapFunction(operationContext, true), true, true, "Final Aggregation");
    }
}
