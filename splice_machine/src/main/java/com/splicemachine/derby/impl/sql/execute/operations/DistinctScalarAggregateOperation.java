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
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.utils.EngineUtils;
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
                                            double optimizerEstimatedCost,
                                            CompilerContext.NativeSparkModeType nativeSparkMode) throws StandardException {
        super(source, aggregateItem, source.getActivation(), rowAllocator, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost, nativeSparkMode);
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
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        OperationContext operationContext = dsp.createOperationContext(this);
        DataSet<ExecRow> dataSet = source.getDataSet(dsp);
        DataSet<ExecRow> dataSetWithNativeSparkAggregation = null;

        if (nativeSparkForced())
            dataSet = dataSet.upgradeToSparkNativeDataSet(operationContext);

        // If the aggregation can be applied using native Spark UnsafeRow, then do so
        // and return immediately.  Otherwise, use traditional Splice lower-level
        // functional APIs.
        if (nativeSparkEnabled())
            dataSetWithNativeSparkAggregation =
                dataSet.applyNativeSparkAggregation(null, aggregates,
                                                    false, operationContext);
        if (dataSetWithNativeSparkAggregation != null) {
            nativeSparkUsed = true;
            return dataSetWithNativeSparkAggregation;
        }

        int numDistinctAggs = 0;
        for (SpliceGenericAggregator aggregator : aggregates) {
            if (!aggregator.isDistinct())
                continue;
            numDistinctAggs++;
        }

        if (numDistinctAggs > 1) {
            /**
             * To handle multiple distinct aggregates, we will be splitting an input row to multiple rows. For example,
             * Given a multiple distinct aggregate query like the following:
             * select a1, count(distinct b1), sum(distinct c1), max(d1) from t1;
             *
             * an input row would look like the below
             * (count_result, count_input(b1), count_func, sum_result, sum_input(c1), sum_func, max_result, max_input(d1), max_func),
             * we will emit 3 rows as follows:
             * (2, count_result, b1, count_func),
             * (5, sum_result, c1, sum_func),
             * (null, max_result, d1, max_func).
             * Here the value 2, 5  or null in each row are the distinct aggregate column's position in the original row
             */
            operationContext.pushScope();
            dataSet = dataSet.map(new CountReadFunction(operationContext));
            operationContext.popScope();

            operationContext.pushScopeForOp(OperationContext.Scope.EXPAND);
            DataSet set1 = dataSet.flatMap(new DistinctAggregatesPrepareFunction(operationContext, null));
            operationContext.popScope();

            operationContext.pushScopeForOp(OperationContext.Scope.GROUP_AGGREGATE_KEYER);
            PairDataSet set2 = set1.keyBy(new DistinctAggregateKeyCreation(operationContext, null));
            operationContext.popScope();

            operationContext.pushScopeForOp(OperationContext.Scope.REDUCE);
            PairDataSet set3 = set2.reduceByKey(
                    new MergeNonDistinctAggregatesFunctionForMixedRows(operationContext,
                            null));
            operationContext.popScope();

            operationContext.pushScopeForOp(OperationContext.Scope.READ);
            DataSet set4 = set3.values(operationContext);
            operationContext.popScope();

            // with more than one distinct aggregates, each row is split into multiple rows
            // where each row starts with the distinct column id
            // Note, groupKeys as input for KeyerFunction is 0-based
            int[] tmpGroupKey = {0};
            operationContext.pushScopeForOp(OperationContext.Scope.GROUP_AGGREGATE_KEYER);
            set2 = set4.keyBy(new KeyerFunction(operationContext, tmpGroupKey));
            operationContext.popScope();

            operationContext.pushScopeForOp(OperationContext.Scope.REDUCE);
            set3 = set2.reduceByKey(new MergeAllAggregatesFunctionForMixedRows(operationContext, null));
            set4 = set3.values(operationContext);
            operationContext.popScope();

            DataSet ds3 = set4.coalesce(1, true, false, operationContext, true, "Coalesce");
            return ds3.mapPartitions(new StitchMixedRowFlatMapFunction(operationContext), true, true, "Stitch Mixed rows");
        } else {
            DataSet<ExecRow> ds2 = dataSet.keyBy(new KeyerFunction(operationContext, keyColumns), null, true, "Prepare Keys")
                    .reduceByKey(new MergeNonDistinctAggregatesFunction(operationContext), false, true, "Reduce")
                    .values(null, false, operationContext, true, "Read Values");
            DataSet<ExecRow> ds3 = ds2.mapPartitions(new MergeAllAggregatesFlatMapFunction(operationContext, false), false, true, "First Aggregation");
            DataSet<ExecRow> ds4 = ds3.coalesce(1, true, false, operationContext, true, "Coalesce");
            return ds4.mapPartitions(new MergeAllAggregatesFlatMapFunction(operationContext, true), true, true, "Final Aggregation");
        }

    }
}
