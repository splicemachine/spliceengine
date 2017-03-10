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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.GroupedAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.NormalizeOperation;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.GroupedAggregateContext;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.apache.commons.collections.iterators.SingletonIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

/**
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
 */
public class DistinctAggregatesPrepareFunction extends SpliceFlatMapFunction<GroupedAggregateOperation, LocatedRow, LocatedRow> {
    private static final long serialVersionUID = 7780564699906451370L;
    private boolean initialized = false;
    private GroupedAggregateContext groupedAggregateContext;
    private int[] groupingKeys;
    private int[] uniqueColumns;

    public DistinctAggregatesPrepareFunction() {
    }

    public DistinctAggregatesPrepareFunction(OperationContext<GroupedAggregateOperation> operationContext) {
        super(operationContext);
    }

    @Override
    public Iterator<LocatedRow> call(LocatedRow sourceRow) throws Exception {
        if (!initialized) {
            GroupedAggregateOperation groupedAggregateOperation = operationContext.getOperation();
            groupedAggregateContext = groupedAggregateOperation.groupedAggregateContext;
            groupingKeys = groupedAggregateContext.getGroupingKeys();
            uniqueColumns = groupedAggregateContext.getNonGroupedUniqueColumns();
        }
        ArrayList<LocatedRow> list = new ArrayList<>(uniqueColumns.length+1);
        for (int i = 0; i < uniqueColumns.length; i++) {
            ValueRow valueRow = new ValueRow(2);
            valueRow.setColumn(1,sourceRow.getRow().getColumn(uniqueColumns[i]));
            valueRow.setColumn(2,sourceRow.getRow().getColumn(uniqueColumns[i+1]));
            list.add(new LocatedRow(valueRow));
        }


/*

        [distinct2 aggregate columns (value, compute)],[distinct position 1],[distinct value],[group by columns]



        * emit N records corresponding to number of distincts + non distinct aggregates
        * [distinct1 aggregate columns (value, compute)],[distinct position 0],[distinct value],[group by columns]
        * [distinct2 aggregate columns (value, compute)],[distinct position 1],[distinct value],[group by columns]
        * [non distinct aggregate columns],[null],[null],[group by columns]




        normalize.source.setCurrentLocatedRow(sourceRow);
        if (sourceRow != null) {
            ExecRow normalized = null;
            try {
                normalized = normalize.normalizeRow(sourceRow.getRow(), true);
            } catch (StandardException e) {
                if (operationContext!=null && operationContext.isPermissive()) {
                    operationContext.recordBadRecord(e.getLocalizedMessage() + sourceRow.toString(), e);
                    return Collections.<LocatedRow>emptyList().iterator();
                }
                throw e;
            }
            getActivation().setCurrentRow(normalized, normalize.getResultSetNumber());
            return new SingletonIterator(new LocatedRow(sourceRow.getRowLocation(), normalized.getClone()));
        }else return Collections.<LocatedRow>emptyList().iterator();
    }
*/
        return null;
    }

}
