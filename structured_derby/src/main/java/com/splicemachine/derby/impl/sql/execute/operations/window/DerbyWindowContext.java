package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.derby.iapi.error.SQLWarningFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.AggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate.GroupedAggregateContext;

/**
 * This class records the window definition (partition, orderby and frame)
 * for a window function.
 *
 * @author Jeff Cunningham
 *         Date: 7/8/14
 */
public class DerbyWindowContext implements GroupedAggregateContext {
    private Activation activation;
    private int partitionItemIdx;
    private int orderingItemIdx;
    private int[] groupingKeys;
    private boolean[] groupingKeyOrder;
    private int[] nonGroupedUniqueColumns;
    private int numDistinctAggs;

    public DerbyWindowContext() {
    }

    public DerbyWindowContext(int partitionItemIdx, int orderingItemIdx) {
        this.partitionItemIdx = partitionItemIdx;
        this.orderingItemIdx = orderingItemIdx;
    }

    @Override
    public void init(SpliceOperationContext context,
                     AggregateContext genericAggregateContext) throws StandardException {
        this.activation = context.getActivation();

        GenericStorablePreparedStatement statement = context.getPreparedStatement();
        ColumnOrdering[] partition = (ColumnOrdering[])
            ((FormatableArrayHolder) (statement.getSavedObject(partitionItemIdx))).getArray(ColumnOrdering.class);

        ColumnOrdering[] orderings = (ColumnOrdering[])
            ((FormatableArrayHolder) (statement.getSavedObject(orderingItemIdx))).getArray(ColumnOrdering.class);

        int[] allKeyedColumns = new int[partition.length + orderings.length];
        boolean[] allSortOrders = new boolean[partition.length + orderings.length];
        int pos=0;
        for(ColumnOrdering partCol:partition){
            allKeyedColumns[pos] = partCol.getColumnId();
            allSortOrders[pos] = partCol.getIsAscending();
            pos++;
        }
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

        groupingKeys = allKeyedColumns;
        groupingKeyOrder = allSortOrders;
        nonGroupedUniqueColumns = new int[]{};
    }

    private boolean keysContain(int[] keyColumns, int inputColNum) {
        for(int keyColumn:keyColumns){
            if(keyColumn==inputColNum)
                return true;
        }
        return false;
    }

    @Override
    public int[] getGroupingKeys() {
        return groupingKeys;
    }

    @Override
    public boolean[] getGroupingKeyOrder() {
        return groupingKeyOrder;
    }

    @Override
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
        out.writeInt(partitionItemIdx);
        out.writeInt(orderingItemIdx);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        partitionItemIdx = in.readInt();
        orderingItemIdx = in.readInt();
    }

}
