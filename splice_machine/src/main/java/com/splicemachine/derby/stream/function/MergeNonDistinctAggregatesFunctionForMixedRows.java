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

package com.splicemachine.derby.stream.function;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.AggregatorInfo;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.GroupedAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.Serializable;
import java.util.List;

/**
 * Created by yxia on 5/31/17.
 */
public class MergeNonDistinctAggregatesFunctionForMixedRows<Op extends SpliceOperation> extends SpliceFunction2<Op, LocatedRow, LocatedRow, LocatedRow> implements Serializable {
    protected SpliceGenericAggregator[] aggregates;
    protected boolean initialized;
    protected int distinctColumnId = 0;

    public MergeNonDistinctAggregatesFunctionForMixedRows() {
    }

    public MergeNonDistinctAggregatesFunctionForMixedRows(OperationContext<Op> operationContext) {
        super(operationContext);
    }

    @Override
    public LocatedRow call(LocatedRow locatedRow1, LocatedRow locatedRow2) throws Exception {
        if (!initialized) {
            setup();
            initialized = true;
        }
        operationContext.recordRead();

        if (locatedRow1 == null) return locatedRow2.getClone();
        if (locatedRow2 == null) return locatedRow1;
        ExecRow r1 = locatedRow1.getRow();
        ExecRow r2 = locatedRow2.getRow();

        // only process the mixed rows corresponding to the non-distinct aggregates
        if (distinctColumnId < 1 || r1.getColumn(distinctColumnId).isNull()) {
            assert aggregates != null: "aggregates is not expected to be null";
            for (SpliceGenericAggregator aggregator : aggregates) {
                if (!aggregator.isDistinct()) {
                    if (!aggregator.isInitialized(locatedRow1.getRow())) {
                        aggregator.initializeAndAccumulateIfNeeded(r1, r1);
                    }
                    if (!aggregator.isInitialized(locatedRow2.getRow())) {
                        aggregator.initializeAndAccumulateIfNeeded(r2, r2);
                    }
                    aggregator.merge(r2, r1);
                }
            }
        }
        return new LocatedRow(locatedRow1.getRowLocation(), r1);
    }

    private void setup() {
        /**
         * With the rows split, the column positions recorded in aggregates are no longer valid,
         * so for multiple distinct aggregate case, we need to compose a new aggregates array with
         * column ids pointing to the new position in the split row.
         */
        GroupedAggregateOperation op = (GroupedAggregateOperation)operationContext.getOperation();
        SpliceGenericAggregator[] origAggregates = op.aggregates;
        int numOfGroupKeys = op.groupedAggregateContext.getGroupingKeys().length;

        List<SpliceGenericAggregator> tmpAggregators = Lists.newArrayList();
        int numOfNonDistinctAggregates = 0;
        ClassFactory cf = op.getActivation().getLanguageConnectionContext().getLanguageConnectionFactory().getClassFactory();
        for (SpliceGenericAggregator aggregator : origAggregates) {
            AggregatorInfo aggInfo = aggregator.getAggregatorInfo();
            AggregatorInfo newAggInfo;
            if (aggregator.isDistinct()) {
                newAggInfo = new AggregatorInfo(aggInfo.getAggregateName()
                        , aggInfo.getAggregatorClassName()
                        , numOfGroupKeys + 2
                        , numOfGroupKeys + 1
                        , numOfGroupKeys + 3
                        , true
                        , aggInfo.getResultDescription());
            } else {
                newAggInfo = new AggregatorInfo(aggInfo.getAggregateName()
                        , aggInfo.getAggregatorClassName()
                        , numOfGroupKeys + numOfNonDistinctAggregates * 3 + 2
                        , numOfGroupKeys + numOfNonDistinctAggregates * 3 + 1
                        , numOfGroupKeys + numOfNonDistinctAggregates * 3 + 3
                        , false
                        , aggInfo.getResultDescription());
                numOfNonDistinctAggregates++;
            }
            tmpAggregators.add(new SpliceGenericAggregator(newAggInfo, cf));
        }
        aggregates = new SpliceGenericAggregator[tmpAggregators.size()];
        tmpAggregators.toArray(aggregates);

        distinctColumnId = numOfGroupKeys + 1;
    }
}
