package com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate;

import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.AggregateContext;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;

import org.apache.derby.iapi.error.SQLWarningFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;

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

    public DerbyGroupedAggregateContext() {
    }

    public DerbyGroupedAggregateContext(int orderingItem) {
        this.orderingItem = orderingItem;
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
        out.writeInt(orderingItem);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        orderingItem = in.readInt();
    }
}
