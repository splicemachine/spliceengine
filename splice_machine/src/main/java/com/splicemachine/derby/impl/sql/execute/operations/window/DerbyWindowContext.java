/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
