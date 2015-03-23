package com.splicemachine.derby.impl.sql.execute.operations;


import com.google.common.base.Strings;
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
import com.splicemachine.derby.impl.ast.PlanPrinter;
import com.splicemachine.derby.impl.storage.RowProviders;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    private Pair<String,Integer>[] plan;
    protected static final String NAME = ExplainOperation.class.getSimpleName().replaceAll("Operation","");
    private static final Pattern pattern = Pattern.compile("n=[0-9]+");

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
            clearState();
            return null;
        }

        currentTemplate.resetRowArray();
        DataValueDescriptor[] dvds = currentTemplate.getRowArray();
        Pair<String,Integer> next;
        do{
            next=plan[pos++];
        }while(pos<plan.length && filterNonExistingNodes(next.getFirst()));

        if(pos>plan.length){
            clearState();
            return null;
        }
        dvds[0].setValue(Strings.repeat(" ",next.getSecond())+next.getFirst());
        return currentTemplate;
    }

    private boolean filterNonExistingNodes(String next){
        /*
         * The query planner will impose a ProjectRestrictNode
         * into the PlanPrinter which will disappear after optimization. This
         * method filters those ProjectRestricts out.
         *
         * In addition, we throw out the ScrollInsensitive if it pops to the top,
         * just to make things look nice and pretty(since Splice doesn't make sense
         * of the SI result set)
         */
        if(next.startsWith("ScrollInsensitive")) return true;
        if(!next.startsWith("ProjectRestrict")) return false;

        //find the corresponding projectrestrict operation
        Matcher rsnStr = pattern.matcher(next);
        int rsn;
        if(rsnStr.find()){
            String match = rsnStr.group();
            rsn = Integer.parseInt(match.substring(2));
            return findOp(source,rsn)==null;
        }else{
            //should never happen
            return true;
        }
    }

    private SpliceOperation findOp(SpliceOperation op,int rsn){
        if(op==null) return null;
        if(op.resultSetNumber()==rsn) return op;
        List<SpliceOperation> children = op.getSubOperations();
        for(SpliceOperation child:children){
            SpliceOperation found = findOp(child,rsn);
            if(found!=null)
                return found;
        }
        return null;
    }

    protected void clearState(){
        Map<String, List<Pair<String,Integer>>> m=PlanPrinter.planMap.get();
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

    @SuppressWarnings("unchecked")
    private void getPlanInformation(){
        Map<String,List<Pair<String,Integer>>> m = PlanPrinter.planMap.get();
        String sql = activation.getPreparedStatement().getSource();
        List<Pair<String,Integer>> opPlanMap = m.get(sql);
        if(opPlanMap!=null){
            plan = new Pair[opPlanMap.size()];
            opPlanMap.toArray(plan);
        }else
            plan = new Pair[]{};
    }

}
