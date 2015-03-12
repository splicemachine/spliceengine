package com.splicemachine.derby.impl.sql.execute.operations;


import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.ast.PlanPrinter;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.management.XPlainPlanNode;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.utils.SpliceLogUtils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import org.apache.log4j.Logger;
import com.splicemachine.db.impl.sql.execute.ValueRow;

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
    private int pos = 0;
    private String[] plan;
    protected static final String NAME = ExplainOperation.class.getSimpleName().replaceAll("Operation","");

	@Override
	public String getName() {
			return NAME;
	}
	
    static {
        nodeTypes = Arrays.asList(NodeType.MAP, NodeType.SCAN);
    }

    public ExplainOperation(SpliceOperation source, Activation activation, int resultSetNumber) throws StandardException{
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

        if (plan == null || pos >= plan.length) {
            Map<String, Map<Integer, String>> m=PlanPrinter.planMap.get();
            String sql = activation.getPreparedStatement().getSource();
            m.remove(sql);
            return null;
        }

        currentTemplate.resetRowArray();
        DataValueDescriptor[] dvds = currentTemplate.getRowArray();
        String next=com.google.common.base.Strings.repeat(" ",pos)+plan[pos++];
        dvds[0].setValue(next);
        return currentTemplate;
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
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException{
        SpliceLogUtils.trace(LOG, "executeScan");
        try {
            RowProvider rowProvider = getMapRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), runtimeContext);
            return new SpliceNoPutResultSet(activation, this, rowProvider);
        }catch(IOException e) {
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

    @Override public void close() throws StandardException,IOException { }

    private void getPlanInformation(){
        Map<String,Map<Integer,String>> m = PlanPrinter.planMap.get();
        String sql = activation.getPreparedStatement().getSource();
        Map<Integer,String> opPlanMap = m.get(sql);
        List<String> printedTree = printOperationTree(opPlanMap);
        plan = new String[printedTree.size()];
        printedTree.toArray(plan);
    }

    private List<String> printOperationTree(Map<Integer, String> baseMap){
        List<String> destination = new LinkedList<>();
        addTo(destination,baseMap,source,0);
        return destination;
    }

    private void addTo(List<String> destination,Map<Integer,String> sourceMap,SpliceOperation source,int level){
        String s=prettyFormat(source,sourceMap.get(source.resultSetNumber()));

        String opString = com.google.common.base.Strings.repeat(" ",level)+s;
        destination.add(opString);
        List<SpliceOperation> subOps = Lists.reverse(source.getSubOperations());
        boolean isFirst = true;
        for(SpliceOperation op:subOps){
            if(isFirst) isFirst = false;
            else{ //add a ---- line to separate different sides of join and union entries
                destination.add("--------");
            }
            addTo(destination,sourceMap,op,level+1);
        }
    }

    private String prettyFormat(SpliceOperation source,String baseString){
        /*
         * This method replaces the Query optimizer-based name (e.g. FromBaseTable, IndexToBaseRowNode, etc)
         * with a more elegant name like "TableScan","IndexLookup","NestedLoopJoin", etc.
         */
        int nameEndIndex = baseString.indexOf("(");
        return baseString.replaceFirst(baseString.substring(0,nameEndIndex),source.getName());
    }

}
