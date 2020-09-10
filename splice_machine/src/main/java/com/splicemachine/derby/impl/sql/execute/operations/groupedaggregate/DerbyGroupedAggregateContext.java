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

package com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate;

import com.splicemachine.db.iapi.error.SQLWarningFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.types.SQLBit;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.AggregateContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import splice.com.google.common.collect.Lists;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 11/4/13
 */
public class DerbyGroupedAggregateContext implements GroupedAggregateContext {
    private Activation activation;
    private int orderingItem;
    private int[] groupingKeys;
    private boolean[] groupingKeyOrder;
    private int[] nonGroupedUniqueColumns;
    private int numDistinctAggs;
    private int groupingIdColPosition;
    private int groupingIdArrayItem;
    private SQLBit[] groupingIdVals;

    public DerbyGroupedAggregateContext() {
    }

    public DerbyGroupedAggregateContext(int orderingItem, int groupingIdColPosition, int groupingIdArrayItem) {
        this.orderingItem = orderingItem;
        this.groupingIdColPosition = groupingIdColPosition;
        this.groupingIdArrayItem = groupingIdArrayItem;
    }

    @Override
    public void init(SpliceOperationContext context,
                     AggregateContext genericAggregateContext) throws StandardException {
        this.activation = context.getActivation();

        GenericStorablePreparedStatement statement = context.getPreparedStatement();
        ColumnOrdering[] orderings = (ColumnOrdering[])
                ((FormatableArrayHolder) (statement.getSavedObject(orderingItem))).getArray(ColumnOrdering.class);

        int[] allKeyedColumns = new int[orderings.length];
        boolean[] allSortOrders = new boolean[orderings.length];
        int pos=0;
        for(ColumnOrdering order:orderings){
            allKeyedColumns[pos] = order.getColumnId();
            allSortOrders[pos] = order.getIsAscending();
            pos++;
        }

        List<Integer> nonUniqueColumns = Lists.newArrayListWithExpectedSize(0);
        SpliceGenericAggregator[] aggregates = genericAggregateContext.getAggregators();
        for(SpliceGenericAggregator aggregate: aggregates){
            if(aggregate.isDistinct()){
                int inputColNum = aggregate.getAggregatorInfo().getInputColNum();
                if(!keysContain(allKeyedColumns,inputColNum)){
                   nonUniqueColumns.add(inputColNum);
               }
               numDistinctAggs++;
            }
        }

        if(numDistinctAggs>0){
            groupingKeys = new int[allKeyedColumns.length-1];
            System.arraycopy(allKeyedColumns,0,groupingKeys,0,groupingKeys.length);
            groupingKeyOrder = new boolean[allSortOrders.length-1];
            System.arraycopy(allSortOrders,0,groupingKeyOrder,0,groupingKeyOrder.length);

            nonUniqueColumns.add(allKeyedColumns[allKeyedColumns.length-1]);
            nonGroupedUniqueColumns = new int[nonUniqueColumns.size()];
            pos=0;
            for(Integer nonUniqueColumn:nonUniqueColumns){
                nonGroupedUniqueColumns[pos] = nonUniqueColumn;
                pos++;
            }
        }else{
            groupingKeys = allKeyedColumns;
            groupingKeyOrder = allSortOrders;
            nonGroupedUniqueColumns = new int[]{};
        }

        if (groupingIdArrayItem > 0) {
            FormatableBitSet[] groupingIdArray = (FormatableBitSet[])
                    ((FormatableArrayHolder) (statement.getSavedObject(groupingIdArrayItem))).getArray(FormatableBitSet.class);
            groupingIdVals = new SQLBit[groupingKeys.length];
            for (int i=0; i<groupingKeys.length; i++) {
                groupingIdVals[i] = new SQLBit(groupingIdArray[i].getByteArray());
            }
        }

    }

    private boolean keysContain(int[] keyColumns, int inputColNum) {
        for(int keyColumn:keyColumns){
            if(keyColumn==inputColNum)
                return true;
        }
        return false;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getGroupingKeys() {
        return groupingKeys;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public boolean[] getGroupingKeyOrder() {
        return groupingKeyOrder;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public int[] getNonGroupedUniqueColumns() {
        return nonGroupedUniqueColumns;
    }

    @Override
    public int getNumDistinctAggregates() {
        return numDistinctAggs;
    }

    @Override
    public void addWarning(String warningState) throws StandardException {
        activation.addWarning(SQLWarningFactory.newSQLWarning(warningState));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(orderingItem);
        out.writeInt(groupingIdColPosition);
        out.writeInt(groupingIdArrayItem);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        orderingItem = in.readInt();
        groupingIdColPosition = in.readInt();
        groupingIdArrayItem = in.readInt();

    }

    @Override
    public int getGroupingIdColumnPosition() {
        return groupingIdColPosition;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "DB-9844")
    public SQLBit[] getGroupingIdVals() {
        return groupingIdVals;
    }
}
