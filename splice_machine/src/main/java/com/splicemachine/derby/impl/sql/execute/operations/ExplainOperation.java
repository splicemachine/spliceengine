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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableArrayHolder;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.compile.DataSetProcessorType;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.ast.PlanPrinter;
import com.splicemachine.db.impl.sql.compile.ExplainNode;
import com.splicemachine.db.impl.sql.compile.QueryTreeNode;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import splice.com.google.common.base.Function;
import splice.com.google.common.collect.Iterators;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.stream.Collectors;

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
    private ExplainNode.SparkExplainKind sparkExplainKind;

    List<String> explainString = new ArrayList<>();
    private int noStatsTablesRef;
    private int noStatsColumnsRef;
    private SQLVarchar[] noStatsTables;
    private SQLVarchar[] noStatsColumns;

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
    public ExplainOperation(SpliceOperation source, Activation activation,
                            int resultSetNumber, String sparkExplainKind,
                            int noStatsTablesRef, int noStatsColumnsRef) throws StandardException {
        super(activation, resultSetNumber, 0, 0);
        this.activation = activation;
        this.source = source;

        if (sparkExplainKind.equals(ExplainNode.SparkExplainKind.EXECUTED.toString()))
            this.sparkExplainKind = ExplainNode.SparkExplainKind.EXECUTED;
        else if (sparkExplainKind.equals(ExplainNode.SparkExplainKind.LOGICAL.toString()))
            this.sparkExplainKind = ExplainNode.SparkExplainKind.LOGICAL;
        else if (sparkExplainKind.equals(ExplainNode.SparkExplainKind.OPTIMIZED.toString()))
            this.sparkExplainKind = ExplainNode.SparkExplainKind.OPTIMIZED;
        else if (sparkExplainKind.equals(ExplainNode.SparkExplainKind.ANALYZED.toString()))
            this.sparkExplainKind = ExplainNode.SparkExplainKind.ANALYZED;
        else
            this.sparkExplainKind = ExplainNode.SparkExplainKind.NONE;

        this.noStatsTablesRef = noStatsTablesRef;
        this.noStatsColumnsRef = noStatsColumnsRef;

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
        if (source != null)
            source.init(context);

        ExecPreparedStatement eps = activation.getPreparedStatement();
        noStatsTables  = (SQLVarchar[]) ((FormatableArrayHolder)eps.getSavedObject(noStatsTablesRef)).getArray(SQLVarchar.class);
        noStatsColumns = (SQLVarchar[]) ((FormatableArrayHolder)eps.getSavedObject(noStatsColumnsRef)).getArray(SQLVarchar.class);
    }

    @Override
    public void openCore() throws StandardException {
        getPlanInformation();
        addNoStatsTablesAndColumns();
        if (sparkExplainKind == ExplainNode.SparkExplainKind.NONE) {
            // We always run explain on control
            openCore(EngineDriver.driver().processorFactory().localProcessor(activation, this));
        } else {
            // Spark explain should be run in Spark
            super.openCore();
        }
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
    public SpliceOperation getLeftOperation() {
        return (SpliceOperation) source;
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
            DataSetProcessorType type = activation.datasetProcessorType();
            explainStringIter = PlanPrinter.planToIterator(opPlanMap, type);
        } else
            explainStringIter = Iterators.emptyIterator();
        while (explainStringIter.hasNext()) {
            explainString.add(explainStringIter.next());
        }
    }

    private void addNoStatsTablesAndColumns() {
        if (noStatsTables.length > 0) {
            explainString.add("Table statistics are missing or skipped for the following tables:");
            explainString.add(
                    Arrays.stream(noStatsTables).map(SQLVarchar::toString).collect(Collectors.joining(", ")));
        }
        if (noStatsColumns.length > 0) {
            explainString.add("Column statistics are missing or skipped for the following columns:");
            explainString.add(
                    Arrays.stream(noStatsColumns).map(SQLVarchar::toString).collect(Collectors.joining(", ")));
        }
    }

    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        OperationContext operationContext = dsp.createOperationContext(this);
        operationContext.pushScope();
        try {
            DataSet<ExecRow> resultDS = null;
            List<String> explainToDisplay = explainString;
            if (sparkExplainKind != ExplainNode.SparkExplainKind.NONE &&
                dsp.getType() == DataSetProcessor.Type.SPARK) {
                dsp.setSparkExplain(sparkExplainKind);
                dsp.resetOpDepth();
                resultDS = source.getResultDataSet(dsp);

                if (resultDS.isNativeSpark())
                    dsp.prependSparkExplainStrings(resultDS.buildNativeSparkExplain(sparkExplainKind), true, true);

                explainToDisplay = dsp.getNativeSparkExplain();
            }
            return dsp.createDataSet(Iterators.transform(explainToDisplay.iterator(), new Function<String, ExecRow>() {
                                                             @Nullable
                                                             @Override
                                                             public ExecRow apply(@Nullable String n) {
                                                                 try {
                                                                     currentTemplate.resetRowArray();
                                                                     DataValueDescriptor[] dvds = currentTemplate.getRowArray();
                                                                     dvds[0].setValue(n);
                                                                     return currentTemplate.getClone();
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
        out.writeUTF(sparkExplainKind.toString());
        if (!sparkExplainKind.equals(ExplainNode.SparkExplainKind.NONE))
            out.writeObject(source);

        out.writeInt(noStatsTablesRef);
        out.writeInt(noStatsColumnsRef);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        int size = in.readInt();
        explainString = new ArrayList<>();
        for (int i = 0; i < size; ++i) {
            explainString.add(in.readUTF());
        }
        String sparkExplainKindString = in.readUTF();
        if (sparkExplainKindString.equals(ExplainNode.SparkExplainKind.EXECUTED.toString()))
            this.sparkExplainKind = ExplainNode.SparkExplainKind.EXECUTED;
        else if (sparkExplainKindString.equals(ExplainNode.SparkExplainKind.LOGICAL.toString()))
            this.sparkExplainKind = ExplainNode.SparkExplainKind.LOGICAL;
        else if (sparkExplainKindString.equals(ExplainNode.SparkExplainKind.OPTIMIZED.toString()))
            this.sparkExplainKind = ExplainNode.SparkExplainKind.OPTIMIZED;
        else if (sparkExplainKindString.equals(ExplainNode.SparkExplainKind.ANALYZED.toString()))
            this.sparkExplainKind = ExplainNode.SparkExplainKind.ANALYZED;
        else
            this.sparkExplainKind = ExplainNode.SparkExplainKind.NONE;
        if (!sparkExplainKind.equals(ExplainNode.SparkExplainKind.NONE))
            source = (SpliceOperation)in.readObject();

        noStatsTablesRef = in.readInt();
        noStatsColumnsRef = in.readInt();
    }
}
