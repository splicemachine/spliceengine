package com.splicemachine.derby.impl.sql.execute.operations;


import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.management.XPlainPlanNode;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.*;
import org.apache.log4j.Logger;

import java.util.*;
import java.io.IOException;

/**
 * Created by jyuan on 6/9/14.
 */
public class ExplainOperation extends SpliceBaseOperation {

    private static Logger LOG = Logger.getLogger(ExplainOperation.class);
    protected SpliceOperation source;
    protected static List<NodeType> nodeTypes;
    protected ExecRow currentTemplate;
    private List<ExecRow> rows;
    private XPlainPlanNode root;
    private HashMap<SpliceOperation, XPlainPlanNode> xPlainPlanMap;
    private static final int INIT_SIZE = 30;
    private int pos;

    static {
        nodeTypes = Arrays.asList(NodeType.MAP, NodeType.SCAN);
    }

    public ExplainOperation(SpliceOperation source, Activation activation, int resultSetNumber) throws StandardException{
        super(activation, resultSetNumber, 0, 0);
        this.activation = activation;
        this.source = source;
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);
        currentTemplate = new ValueRow(1);
        currentTemplate.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
        rows = new ArrayList<ExecRow>(INIT_SIZE);
        xPlainPlanMap = new HashMap<SpliceOperation, XPlainPlanNode>(INIT_SIZE);
        pos = 0;
        constructExplainPlan();
        printExplainPlan(root);
    }

    private void printExplainPlan(XPlainPlanNode n) throws StandardException{
        currentTemplate.resetRowArray();
        DataValueDescriptor[] dvds = currentTemplate.getRowArray();
        dvds[0].setValue(n.toString());
        rows.add(currentTemplate.getClone());

        List<XPlainPlanNode> children = n.getChildren();
        for (int i = 0; i < children.size(); ++i) {
            printExplainPlan(children.get(i));
        }
    }

    private void constructExplainPlan() {
        Queue<SpliceOperation> operationQueue = new LinkedList<SpliceOperation>();
        operationQueue.add(source);
        XPlainPlanNode node = new XPlainPlanNode(source.getName(),
                source.getEstimatedRowCount(), source.getEstimatedCost(), 0);
        xPlainPlanMap.put(source, node);
        root = node;
        while(!operationQueue.isEmpty()) {
            SpliceOperation op = operationQueue.remove();
            XPlainPlanNode parent = xPlainPlanMap.get(op);
            int level = parent.getLevel();

            List<SpliceOperation> subOperations = op.getSubOperations();
            for (int i = 0; i < subOperations.size(); ++i) {
                SpliceOperation subOperation = subOperations.get(i);
                node = new XPlainPlanNode(subOperation.getName(),
                        subOperation.getEstimatedRowCount(), subOperation.getEstimatedCost(), level + 1);

                parent.addChild(node);
                xPlainPlanMap.put(subOperation, node);
                operationQueue.add(subOperation);
            }
        }
    }

    @Override
    public void open() throws StandardException, IOException {

    }

    public List<NodeType> getNodeTypes() {
        return nodeTypes;
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if (pos < rows.size()) {
            int i = pos++;
            return rows.get(i);
        }
        else {
            return null;
        }
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
        return Arrays.asList(source);
    }

    @Override
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeScan");
        return new SpliceNoPutResultSet(activation, this, getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), runtimeContext));
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
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
    public void close() throws StandardException,IOException {
    }
}
