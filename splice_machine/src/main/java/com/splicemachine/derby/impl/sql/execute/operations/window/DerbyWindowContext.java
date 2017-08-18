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

package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.db.iapi.error.SQLWarningFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.db.impl.sql.execute.WindowFunctionInfo;
import com.splicemachine.db.impl.sql.execute.WindowFunctionInfoList;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.SpliceMethod;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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
    private WindowAggregator[] windowAggregators;
    private SpliceMethod<ExecRow> rowAllocator;
    private ExecRow sortTemplateRow;
    private ExecRow sourceExecIndexRow;

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

        ClassFactory classFactory = context.getActivation()
                .getLanguageConnectionContext()
                .getLanguageConnectionFactory()
                .getClassFactory();
        this.windowAggregators = buildWindowAggregators((WindowFunctionInfoList)statement.getSavedObject(aggregateItem),
                                                         classFactory);
        this.rowAllocator = (rowAllocatorMethodName==null)? null: new SpliceMethod<ExecRow>(rowAllocatorMethodName,activation);
    }

    @Override
    public void addWarning(String warningState) throws StandardException {
        activation.addWarning(SQLWarningFactory.newSQLWarning(warningState));
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public WindowAggregator[] getWindowFunctions() {
        return windowAggregators;
    }

    @Override
    public ExecRow getSortTemplateRow() throws StandardException {
        if(sortTemplateRow==null){
            sortTemplateRow = activation.getExecutionFactory().getIndexableRow(rowAllocator.invoke());
        }
        return sortTemplateRow;
    }

    @Override
    public ExecRow getSourceIndexRow() {
        if(sourceExecIndexRow==null){
            sourceExecIndexRow = activation.getExecutionFactory().getIndexableRow(sortTemplateRow);
        }
        return sourceExecIndexRow;
    }

    @Override
    public int[] getKeyColumns() {
        // Any and all aggregators in a window context share the same over() clause
        return this.windowAggregators[0].getKeyColumns();
    }

    @Override
    public boolean[] getKeyOrders() {
        // Any and all aggregators in a window context share the same over() clause
        return this.windowAggregators[0].getKeyOrders();
    }

    @Override
    public int[] getPartitionColumns() {
        // Any and all aggregators in a window context share the same over() clause
        return this.windowAggregators[0].getPartitionColumns();
    }

    @Override
    public FrameDefinition getFrameDefinition() {
        return this.windowAggregators[0].getFrameDefinition();
    }

    @Override
    public int[] getSortColumns() {
        return this.windowAggregators[0].getSortColumns();
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

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder("WindowContext{");
        for (WindowAggregator wa : windowAggregators) {
            buf.append(wa.toString());
        }
        buf.append("}");
        return buf.toString();
    }

    private static WindowAggregator[] buildWindowAggregators(WindowFunctionInfoList infos, ClassFactory cf) {
        WindowAggregator[] windowAggregators = new WindowAggregator[infos.size()];
        int i=0;
        for (WindowFunctionInfo info : infos){
            // WindowFunctionInfos batched into same context only differ by
            // their functions implementations; all over() clauses are identical
            windowAggregators[i++] = new WindowAggregatorImpl(info, cf);
        }
        return windowAggregators;
    }
}
