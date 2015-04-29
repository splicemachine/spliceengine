package com.splicemachine.derby.impl.sql.execute.operations;


import com.google.common.collect.Iterators;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.ast.ExplainTree;
import com.splicemachine.derby.impl.ast.PlanPrinter;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.stream.DataSet;
import com.splicemachine.derby.stream.DataSetProcessor;
import com.splicemachine.derby.stream.StreamUtils;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import org.sparkproject.guava.common.base.Function;
import org.sparkproject.guava.common.collect.Iterables;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * @author Jun Yuan
 * Date: 6/9/14
 */
public class ExplainOperation extends SpliceBaseOperation {

    private static Logger LOG = Logger.getLogger(ExplainOperation.class);
    protected SpliceOperation source;
    protected static List<NodeType> nodeTypes;
    protected ExecRow currentTemplate;
    private int pos = 0;
    protected static final String NAME = ExplainOperation.class.getSimpleName().replaceAll("Operation", "");
    private Iterator<String> explainStringIter;

    @Override
    public String getName() {
        return NAME;
    }

    static {
        nodeTypes = Arrays.asList(NodeType.MAP, NodeType.SCAN);
    }

    public ExplainOperation(SpliceOperation source, Activation activation, int resultSetNumber) throws StandardException {
        super(activation, resultSetNumber, 0, 0);
        this.activation = activation;
        this.source = source;
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        activation.setTraced(false);
        currentTemplate = new ValueRow(1);
        currentTemplate.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
        getPlanInformation();
    }

    @Override
    public void open() throws StandardException, IOException {

    }

    public List<NodeType> getNodeTypes() {
        return nodeTypes;
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {

        if (!this.explainStringIter.hasNext()) {
            clearState();
            return null;
        }
        String n = explainStringIter.next();

        currentTemplate.resetRowArray();
        DataValueDescriptor[] dvds = currentTemplate.getRowArray();

        dvds[0].setValue(n);
        return currentTemplate;
    }

    protected void clearState() {
        Map<String, ExplainTree> m = PlanPrinter.planMap.get();
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
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeScan");
        try {
            RowProvider rowProvider = getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), runtimeContext);
            return new SpliceNoPutResultSet(activation, this, rowProvider);
        } catch (IOException e) {
            throw StandardException.newException(e.toString());
        }
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        SpliceLogUtils.trace(LOG, "getMapRowProvider,top=%s", top);
        top.init(SpliceOperationContext.newContext(activation));

        //make sure the runtime context knows it can be merged
        spliceRuntimeContext.addPath(resultSetNumber, SpliceRuntimeContext.Side.MERGED);
        return RowProviders.openedSourceProvider(top, LOG, spliceRuntimeContext);
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return currentTemplate;
    }

    @Override
    public void close() throws StandardException, IOException {
    }

    @SuppressWarnings("unchecked")
    private void getPlanInformation() {
        Map<String, ExplainTree> m = PlanPrinter.planMap.get();
        String sql = activation.getPreparedStatement().getSource();
        ExplainTree opPlanMap = m.get(sql);
        if (opPlanMap != null) {
            explainStringIter = opPlanMap.treeToString();
        } else
            explainStringIter = Iterators.emptyIterator();
    }

    public DataSet<LocatedRow> getDataSet(SpliceRuntimeContext spliceRuntimeContext, DataSetProcessor dsp) throws StandardException {
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
                                                                     return new LocatedRow(currentTemplate);
                                                                 } catch (Exception e) {
                                                                     throw new RuntimeException(e);
                                                                 }
                                                             }
                                                         }
                    )
            );
        } finally {
            clearState();
        }
    }


}