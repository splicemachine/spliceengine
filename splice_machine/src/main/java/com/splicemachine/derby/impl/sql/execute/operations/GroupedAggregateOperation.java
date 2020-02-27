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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.DerbyGroupedAggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.GroupedAggregateContext;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 *
 * Multiple Distinct Design
 *
 * Mixed record representation...
 *
 * (1) Remove Need for Join on the back end
 * (2) Do not traverse the base data twice.
 * (3) execution vs. planning strategy?
 *
 * emit N records corresponding to number of distincts + non distinct aggregates
 * [distinct1 aggregate columns (value, compute)],[distinct position 0],[distinct value],[group by columns]
 * [distinct2 aggregate columns (value, compute)],[distinct position 1],[distinct value],[group by columns]
 * [non distinct aggregate columns],[null],[null],[group by columns]
 *
 * keyBy distinctPosition,group by columns
 * flatMap --> apply distincts and non distincts based on key
 * key by group by columns
 * merge back together
 *
 *
 */
public class GroupedAggregateOperation extends GenericAggregateOperation {
    private static final long serialVersionUID = 1l;
    private static Logger LOG = Logger.getLogger(GroupedAggregateOperation.class);
    protected boolean isRollup;
    public GroupedAggregateContext groupedAggregateContext;
    protected static final String NAME = GroupedAggregateOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}


    public GroupedAggregateOperation() {
        super();
        SpliceLogUtils.trace(LOG, "instantiate without parameters");
    }

    @SuppressWarnings("UnusedParameters")
    public GroupedAggregateOperation(
                                        SpliceOperation s,
                                        boolean isInSortedOrder,
                                        int aggregateItem,
                                        Activation a,
                                        GeneratedMethod ra,
                                        int maxRowSize,
                                        int resultSetNumber,
                                        double optimizerEstimatedRowCount,
                                        double optimizerEstimatedCost,
                                        boolean isRollup,
                                        CompilerContext.NativeSparkModeType nativeSparkMode,
                                        GroupedAggregateContext groupedAggregateContext) throws
                                                                                         StandardException {
        super(s, aggregateItem, a, ra, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost, nativeSparkMode);
        this.isRollup = isRollup;
        this.groupedAggregateContext = groupedAggregateContext;
    }

    public GroupedAggregateOperation(SpliceOperation s,
                                     boolean isInSortedOrder,
                                     int aggregateItem,
                                     int orderingItem,
                                     Activation a,
                                     GeneratedMethod ra,
                                     int maxRowSize,
                                     int resultSetNumber,
                                     double optimizerEstimatedRowCount,
                                     double optimizerEstimatedCost,
                                     boolean isRollup,
                                     int groupingIdColPosition,
                                     int groupingIdArrayItem,
                                     CompilerContext.NativeSparkModeType nativeSparkMode) throws StandardException {
        this(s, isInSortedOrder, aggregateItem, a, ra, maxRowSize, resultSetNumber,
                optimizerEstimatedRowCount, optimizerEstimatedCost, isRollup, nativeSparkMode,
                new DerbyGroupedAggregateContext(orderingItem, groupingIdColPosition, groupingIdArrayItem));
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
                                                    ClassNotFoundException {
        super.readExternal(in);
        isRollup = in.readBoolean();
        groupedAggregateContext = (GroupedAggregateContext) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(isRollup);
        out.writeObject(groupedAggregateContext);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException,
                                                            IOException {
        SpliceLogUtils.trace(LOG, "init called");
        super.init(context);
        source.init(context);
        groupedAggregateContext.init(context, aggregateContext);
    }

    @Override
    public ExecRow getExecRowDefinition() {
        SpliceLogUtils.trace(LOG, "getExecRowDefinition");
        return sourceExecIndexRow.getClone();
    }

    @Override
    public String toString() {
        return String.format("GroupedAggregateOperation {resultSetNumber=%d, source=%s}", resultSetNumber, source);
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "Grouped" + super.prettyPrint(indentLevel);
    }

    @Override
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        OperationContext<GroupedAggregateOperation> operationContext = dsp.createOperationContext(this);
        dsp.incrementOpDepth();
        DataSet set = source.getDataSet(dsp);
        DataSet sourceDS = set;
        dsp.decrementOpDepth();
        DataSet dataSetWithNativeSparkAggregation = null;

        if (nativeSparkForced() && (isRollup || aggregates.length > 0))
            set = set.upgradeToSparkNativeDataSet(operationContext);

        operationContext.pushScope();
        set = set.map(new CountReadFunction(operationContext));
        operationContext.popScope();

        // Have distinct Aggregates?
        boolean hasMultipleDistinct = false;
        int numOfGroupKeys = groupedAggregateContext.getGroupingKeys().length;
        int[] extendedGroupBy = groupedAggregateContext.getGroupingKeys();

        // If the aggregation can be applied using native Spark UnsafeRow, then do so
        // and return immediately.  Otherwise, use traditional Splice lower-level
        // functional APIs.
        if (nativeSparkEnabled())
            dataSetWithNativeSparkAggregation =
                set.applyNativeSparkAggregation(extendedGroupBy, aggregates,
                                                isRollup, operationContext);
        if (dataSetWithNativeSparkAggregation != null) {
            dsp.finalizeTempOperationStrings();
            nativeSparkUsed = true;
            return dataSetWithNativeSparkAggregation;
        }

        if (isRollup) {
            extendedGroupBy = new int[numOfGroupKeys+1];
            for (int i=0; i<numOfGroupKeys; i++)
                extendedGroupBy[i] = groupedAggregateContext.getGroupingKeys()[i];
            extendedGroupBy[numOfGroupKeys] = groupedAggregateContext.getGroupingIdColumnPosition();
        }

        if (isRollup) { // OLAP Rollup Functionality
            operationContext.pushScopeForOp(OperationContext.Scope.ROLLUP);
            set = set.flatMap(new GroupedAggregateRollupFlatMapFunction(operationContext));
            operationContext.popScope();
        }

        if (groupedAggregateContext.getNonGroupedUniqueColumns() != null && groupedAggregateContext.getNonGroupedUniqueColumns().length > 0) {
            if (groupedAggregateContext.getNonGroupedUniqueColumns().length > 1) {
                /**
                 * To handle multiple distinct aggregates, we will be splitting an input row to multiple rows. For example,
                 * Given a multiple distinct aggregate query like the following:
                 * select a1, count(distinct b1), sum(distinct c1), max(d1) from t1 group by a1;
                 *
                 * an input row would look like the below
                 * (a1, count_result, count_input(b1), count_func, sum_result, sum_input(c1), sum_func, max_result, max_input(d1), max_func),
                 * we will emit 3 rows as follows:
                 * (a1, 3, count_result, b1, count_func),
                 * (a1, 6, sum_result, c1, sum_func),
                 * (a1, null, max_result, d1, max_func).
                 * Here the value 3, 6  or null in each row are the distinct aggregate column's position in the original row
                 */
                hasMultipleDistinct = true;
                operationContext.pushScopeForOp(OperationContext.Scope.EXPAND);
                DataSet set1 = set.flatMap(new DistinctAggregatesPrepareFunction(operationContext, extendedGroupBy));
                operationContext.popScope();

                operationContext.pushScopeForOp(OperationContext.Scope.GROUP_AGGREGATE_KEYER);
                PairDataSet set2 = set1.keyBy(new DistinctAggregateKeyCreation(operationContext, extendedGroupBy));

                operationContext.popScope();

                operationContext.pushScopeForOp(OperationContext.Scope.REDUCE);
                PairDataSet set3 = set2.reduceByKey(
                        new MergeNonDistinctAggregatesFunctionForMixedRows(operationContext, extendedGroupBy));
                operationContext.popScope();

                operationContext.pushScopeForOp(OperationContext.Scope.READ);
                DataSet set4 = set3.values(operationContext);
                operationContext.popScope();
                set = set4;
            } else {
                //only one distinct aggregate
                int[] allKeys = ArrayUtils.addAll(extendedGroupBy, groupedAggregateContext.getNonGroupedUniqueColumns());

                operationContext.pushScopeForOp(OperationContext.Scope.GROUP_AGGREGATE_KEYER);
                PairDataSet set2 = set.keyBy(new KeyerFunction(operationContext, allKeys));
                operationContext.popScope();

                operationContext.pushScopeForOp(OperationContext.Scope.REDUCE);
                PairDataSet set3 = set2.reduceByKey(new MergeNonDistinctAggregatesFunction(operationContext));
                operationContext.popScope();

                operationContext.pushScopeForOp(OperationContext.Scope.READ);
                DataSet set4 = set3.values(operationContext);
                operationContext.popScope();
                set = set4;
            }
        }
        
        // with more than one distinct aggregates, each row is split into multiple rows
        // where each row starts with the original group keys + the distinct column id
        operationContext.pushScopeForOp(OperationContext.Scope.GROUP_AGGREGATE_KEYER);
        PairDataSet set2;
        if (hasMultipleDistinct) {
            int[] tmpGroupKey = new int[extendedGroupBy.length + 1];
            //Note, groupKeys as input for KeyerFunction is 0-based
            for (int i=0; i<extendedGroupBy.length+1; i++)
                tmpGroupKey[i] = i;
            set2 = set.keyBy(new KeyerFunction(operationContext, tmpGroupKey), operationContext);
        }
        else {
            set2 = set.keyBy(new KeyerFunction(operationContext, extendedGroupBy), operationContext);
        }
        operationContext.popScope();
        
        operationContext.pushScopeForOp(OperationContext.Scope.REDUCE);
        PairDataSet set3;
        if (hasMultipleDistinct)
            set3 = set2.reduceByKey(new MergeAllAggregatesFunctionForMixedRows(operationContext, extendedGroupBy));
        else
            set3 = set2.reduceByKey(new MergeAllAggregatesFunction(operationContext));
        operationContext.popScope();
        
        operationContext.pushScopeForOp(OperationContext.Scope.READ);
        DataSet set4 = set3.values(OperationContext.Scope.READ.displayName(), operationContext);
        operationContext.popScope();

        //need to stitch the split rows together for multiple aggregates case
        if (hasMultipleDistinct) {
            operationContext.pushScopeForOp(OperationContext.Scope.GROUP_AGGREGATE_KEYER);
            PairDataSet set6 = set4.keyBy(new KeyerFunction(operationContext, extendedGroupBy), operationContext);
            operationContext.popScope();

            operationContext.pushScopeForOp(OperationContext.Scope.REDUCE);
            PairDataSet set7 = set6.reduceByKey(new StitchMixedRowFunction(operationContext, extendedGroupBy));
            operationContext.popScope();

            operationContext.pushScopeForOp(OperationContext.Scope.READ);
            set4 = set7.values(OperationContext.Scope.READ.displayName(), operationContext);
            operationContext.popScope();
        }
        
        operationContext.pushScopeForOp(OperationContext.Scope.FINALIZE);
        DataSet set5 = set4.map(new AggregateFinisherFunction(operationContext), true);
        operationContext.popScope();
        handleSparkExplain(set5, sourceDS, dsp);

        return set5;
    }

}
