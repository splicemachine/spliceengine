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

package com.splicemachine.derby.impl.sql.execute.operations;

import org.spark_project.guava.base.Function;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import org.spark_project.guava.collect.Iterators;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.ast.PlanPrinter;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

/**
 *
 * Operation for explain plans.  Top operation on the stack for
 *
 * "explain <statement>"
 *
 * @author Jun Yuan
 * Date: 6/9/14
 */
public class ExplainOperation extends SpliceBaseOperation {
    protected static final String NAME = ExplainOperation.class.getSimpleName().replaceAll("Operation", "");
    protected SpliceOperation source;
    protected ExecRow currentTemplate;

    List<String> explainString = new ArrayList<>();

    /**
     *
     * Static name for the explain operation.
     *
     * @return
     */
    @Override
    public String getName() {
        return NAME;
    }

    /**
     *
     * No Op Constructor for the operation.  Required for serde.
     *
     */
    public ExplainOperation(){ }

    /**
     *
     * Simple constructor.
     *
     * @param source
     * @param activation
     * @param resultSetNumber
     * @throws StandardException
     */
    public ExplainOperation(SpliceOperation source, Activation activation, int resultSetNumber) throws StandardException {
        super(activation, resultSetNumber, 0, 0);
        this.activation = activation;
        this.source = source;
        init();
    }

    /**
     *
     * Called after construction or serialization.
     *
     * @param context
     * @throws StandardException
     * @throws IOException
     */
    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        currentTemplate = new ValueRow(1);
        currentTemplate.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
    }

    @Override
    public void openCore() throws StandardException {
        getPlanInformation();
        super.openCore();
    }

    @Override
    public void close() throws StandardException {
        clearState();
        super.close();
    }

    protected void clearState() {
        Map<String, Collection<QueryTreeNode>> m = PlanPrinter.planMap.get();
        String sql = activation.getPreparedStatement().getSource();
        m.remove(sql);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return source.isReferencingTable(tableNumber);
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "Explain:";
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) {
        return null;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.singletonList(source);
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return currentTemplate;
    }

    @SuppressWarnings("unchecked")
    private void getPlanInformation() throws StandardException {
        Map<String, Collection<QueryTreeNode>> m = PlanPrinter.planMap.get();
        String sql = activation.getPreparedStatement().getSource();
        Iterator<String> explainStringIter;
        Collection<QueryTreeNode> opPlanMap = m.get(sql);
        if (opPlanMap != null) {
            CompilerContext.DataSetProcessorType type = activation.getLanguageConnectionContext().getDataSetProcessorType();
            boolean useSpark = (type == CompilerContext.DataSetProcessorType.FORCED_SPARK ||
                    type == CompilerContext.DataSetProcessorType.SPARK);

            if (!useSpark)
                useSpark = PlanPrinter.shouldUseSpark(opPlanMap);

            explainStringIter = PlanPrinter.planToIterator(opPlanMap, useSpark);
        } else
            explainStringIter = Iterators.emptyIterator();
        while (explainStringIter.hasNext()) {
            explainString.add(explainStringIter.next());
        }
    }

    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        OperationContext operationContext = dsp.createOperationContext(this);
        operationContext.pushScope();
        try {
            return dsp.createDataSet(Iterators.transform(explainString.iterator(), new Function<String, LocatedRow>() {
                                                             @Nullable
                                                             @Override
                                                             public LocatedRow apply(@Nullable String n) {
                                                                 try {
                                                                     currentTemplate.resetRowArray();
                                                                     DataValueDescriptor[] dvds = currentTemplate.getRowArray();
                                                                     dvds[0].setValue(n);
                                                                     return new LocatedRow(currentTemplate.getClone());
                                                                 } catch (Exception e) {
                                                                     throw new RuntimeException(e);
                                                                 }
                                                             }
                                                         }
                    ),
                    "Prepare Explain Plan"
            );
        } finally {
            operationContext.popScope();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(explainString.size());
        for (int i = 0; i < explainString.size(); ++i) {
            out.writeUTF(explainString.get(i));
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        int size = in.readInt();
        explainString = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            explainString.add(in.readUTF());
        }
    }
}
