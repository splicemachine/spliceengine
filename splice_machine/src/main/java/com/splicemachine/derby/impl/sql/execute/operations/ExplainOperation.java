package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Function;
import org.sparkproject.guava.collect.Iterables;
import org.sparkproject.guava.collect.Iterators;
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
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.derby.stream.iapi.OperationContext;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.*;

/**
 * @author Jun Yuan
 * Date: 6/9/14
 */
public class ExplainOperation extends SpliceBaseOperation {
    protected static final String NAME = ExplainOperation.class.getSimpleName().replaceAll("Operation", "");
    protected SpliceOperation source;
    protected ExecRow currentTemplate;
    private Iterator<String> explainStringIter;

    @Override
    public String getName() {
        return NAME;
    }

    public ExplainOperation(){ }

    public ExplainOperation(SpliceOperation source, Activation activation, int resultSetNumber) throws StandardException {
        super(activation, resultSetNumber, 0, 0);
        this.activation = activation;
        this.source = source;
        try {
            init(SpliceOperationContext.newContext(activation));
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

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

    protected void clearState(){
        Map<String, Collection<QueryTreeNode>> m=PlanPrinter.planMap.get();
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
    private void getPlanInformation() throws StandardException{
        Map<String,Collection<QueryTreeNode>> m = PlanPrinter.planMap.get();
        String sql = activation.getPreparedStatement().getSource();
        Collection<QueryTreeNode> opPlanMap = m.get(sql);
        if(opPlanMap!=null){
            explainStringIter = PlanPrinter.planToIterator(opPlanMap);
        }else
            explainStringIter =Iterators.emptyIterator();
    }

    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException {
        OperationContext operationContext = dsp.createOperationContext(this);
        operationContext.pushScope();
        try {
            return dsp.createDataSet(Iterables.transform(new Iterable<String>() {
                                                             @Override
                                                             public Iterator<String> iterator() {
                                                                 return explainStringIter;
                                                             }
                                                         }, new Function<String, LocatedRow>() {
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
}
