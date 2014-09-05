package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.derby.iapi.error.SQLWarningFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.derby.impl.sql.execute.WindowFunctionInfo;
import org.apache.derby.impl.sql.execute.WindowFunctionInfoList;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;

/**
 * This class records the window definition (partition, orderby and frame)
 * for a window function.
 *
 * @author Jeff Cunningham
 *         Date: 7/8/14
 */
public class DerbyWindowContext implements WindowContext {
    private String rowAllocatorMethodName;
    private int aggregateItem;
    private Activation activation;
    private int[] partitionColumns;
    private int[] sortColumns;
    private boolean[] sortOrders;
    private int[] keyColumns;
    private boolean[] keyOrders;
    private FrameDefinition frameDefinition;
    private WindowAggregator[] windowAggregators;
    private SpliceMethod<ExecRow> rowAllocator;
    private ExecIndexRow sortTemplateRow;
    private ExecIndexRow sourceExecIndexRow;

    public DerbyWindowContext() {
    }

    public DerbyWindowContext(String rowAllocatorMethodName, int aggregateItem) {
        this.rowAllocatorMethodName = rowAllocatorMethodName;
        this.aggregateItem = aggregateItem;
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        this.activation = context.getActivation();

        GenericStorablePreparedStatement statement = context.getPreparedStatement();
        WindowFunctionInfoList windowFunctionInfos = (WindowFunctionInfoList)statement.getSavedObject(aggregateItem);
        // TODO: this will have to change when we support > 1 window function per query
        // (now using one partition/orderby/frame for all functions in array)
        WindowFunctionInfo theInfo = windowFunctionInfos.firstElement();

        ColumnOrdering[] partition = theInfo.getPartitionInfo();

        ColumnOrdering[] orderings = theInfo.getOrderByInfo();

        frameDefinition = FrameDefinition.create(theInfo.getFrameInfo());

        keyColumns = new int[partition.length + orderings.length];
        keyOrders = new boolean[partition.length + orderings.length];

        partitionColumns = new int[partition.length];
        sortColumns = new int[orderings.length];

        int pos=0;
        for(ColumnOrdering partCol:partition){
            partitionColumns[pos] = partCol.getColumnId();
            keyColumns[pos] = partCol.getColumnId();
            keyOrders[pos] = partCol.getIsAscending();
            pos++;
        }
        pos = 0;
        for(ColumnOrdering order:orderings){
            sortColumns[pos] = order.getColumnId();
            keyColumns[partition.length + pos] = order.getColumnId();
            keyOrders[partition.length + pos] = order.getIsAscending();
            pos++;
        }

        this.windowAggregators = buildWindowAggregators(windowFunctionInfos,
                                                        context.getLanguageConnectionContext().getLanguageConnectionFactory().getClassFactory());
        this.rowAllocator = (rowAllocatorMethodName==null)? null: new SpliceMethod<ExecRow>(rowAllocatorMethodName,activation);
    }

    @Override
    public int[] getPartitionColumns() {
        return partitionColumns;
    }

    @Override
    public int[] getSortColumns() {
        return sortColumns;
    }

    @Override
    public boolean[] getSortOrders() {
        return sortOrders;
    }


    @Override
    public void addWarning(String warningState) throws StandardException {
        activation.addWarning(SQLWarningFactory.newSQLWarning(warningState));
    }

    @Override
    public int[] getKeyColumns() {
        return keyColumns;
    }

    @Override
    public boolean[] getKeyOrders() {
        return keyOrders;
    }

    @Override
    public FrameDefinition getFrameDefinition() {
        return frameDefinition;
    }

    @Override
    public WindowAggregator[] getWindowFunctions() {
        return windowAggregators;
    }

    @Override
    public ExecIndexRow getSortTemplateRow() throws StandardException {
        if(sortTemplateRow==null){
            sortTemplateRow = activation.getExecutionFactory().getIndexableRow(rowAllocator.invoke());
        }
        return sortTemplateRow;
    }

    @Override
    public ExecIndexRow getSourceIndexRow() {
        if(sourceExecIndexRow==null){
            sourceExecIndexRow = activation.getExecutionFactory().getIndexableRow(sortTemplateRow);
        }
        return sourceExecIndexRow;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(rowAllocatorMethodName!=null);
        if(rowAllocatorMethodName!=null)
            out.writeUTF(rowAllocatorMethodName);

        out.writeInt(aggregateItem);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if(in.readBoolean())
            this.rowAllocatorMethodName = in.readUTF();
        else
            this.rowAllocatorMethodName = null;

        this.aggregateItem = in.readInt();
    }

    private static WindowAggregator[] buildWindowAggregators(WindowFunctionInfoList infos, ClassFactory cf) {
        List<WindowAggregator> tmpAggregators = Lists.newArrayList();
        for (WindowFunctionInfo info : infos){
            tmpAggregators.add(new WindowAggregator(info, cf));
        }
        WindowAggregator[] aggregators = new WindowAggregator[tmpAggregators.size()];
        tmpAggregators.toArray(aggregators);
        return aggregators;

    }
}
