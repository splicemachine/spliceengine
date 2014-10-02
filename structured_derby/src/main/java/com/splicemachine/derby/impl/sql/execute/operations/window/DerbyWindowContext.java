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
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.derby.impl.sql.execute.WindowFunctionInfo;
import org.apache.derby.impl.sql.execute.WindowFunctionInfoList;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

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
    private static Logger LOG = Logger.getLogger(DerbyWindowContext.class);


    private String rowAllocatorMethodName;
    private int aggregateItem;
    private Activation activation;
    private List<WindowAggregator> windowAggregators;
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

        this.windowAggregators = buildWindowAggregators((WindowFunctionInfoList)statement.getSavedObject(aggregateItem),
                                                        context.getLanguageConnectionContext().getLanguageConnectionFactory().getClassFactory());
        this.rowAllocator = (rowAllocatorMethodName==null)? null: new SpliceMethod<ExecRow>(rowAllocatorMethodName,activation);
    }

    @Override
    public void addWarning(String warningState) throws StandardException {
        activation.addWarning(SQLWarningFactory.newSQLWarning(warningState));
    }

    @Override
    public List<WindowAggregator> getWindowFunctions() {
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
    public int[] getKeyColumns() {
        // Any and all aggregators in a window context share the same over() clause
        return this.windowAggregators.get(0).getKeyColumns();
    }

    @Override
    public boolean[] getKeyOrders() {
        // Any and all aggregators in a window context share the same over() clause
        return this.windowAggregators.get(0).getKeyOrders();
    }

    @Override
    public int[] getPartitionColumns() {
        // Any and all aggregators in a window context share the same over() clause
        return this.windowAggregators.get(0).getPartitionColumns();
    }

    @Override
    public FrameDefinition getFrameDefinition() {
        return this.windowAggregators.get(0).getFrameDefinition();
    }

    @Override
    public int[] getSortColumns() {
        return this.windowAggregators.get(0).getSortColumns();
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

    private static List<WindowAggregator> buildWindowAggregators(WindowFunctionInfoList infos, ClassFactory cf) {
        List<WindowAggregator> windowAggregators = Lists.newArrayList();
        for (WindowFunctionInfo info : infos){
            // WindowFunctionInfos batched into same context only differ by
            // their functions implementations; all over() clauses are identical
            windowAggregators.add(new WindowAggregatorImpl(info, cf));
        }

        return windowAggregators;
    }
}
