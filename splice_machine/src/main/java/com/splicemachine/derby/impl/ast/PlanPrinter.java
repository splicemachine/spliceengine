package com.splicemachine.derby.impl.ast;

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.derby.impl.sql.execute.operations.IndexRowToBaseRowOperation;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.*;


/**
 * @author P Trolard
 *         Date: 30/09/2013
 */
public class PlanPrinter extends AbstractSpliceVisitor {

    public static Logger LOG = Logger.getLogger(PlanPrinter.class);
    public static Logger PLAN_LOG = Logger.getLogger(PlanPrinter.class.getName() + ".JSONLog");

    public static final String spaces = "  ";
    private boolean explain = false;
    public static ThreadLocal<Map<String,ExplainTree>> planMap = new ThreadLocal<Map<String,ExplainTree>>(){
        @Override
        protected Map<String,ExplainTree> initialValue(){
            return new HashMap<>();
        }
    };

    // Only visit root node

    @Override
    public boolean isPostOrder() {
        return false;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        return true;
    }

    @Override
    public Visitable visit(ExplainNode node) throws StandardException{
        explain = true;
        return visit(node.getPlanRoot());
    }

    @Override
    public Visitable defaultVisit(Visitable node) throws StandardException {

        ResultSetNode rsn;
        if ((explain || LOG.isInfoEnabled() || PLAN_LOG.isTraceEnabled()) &&
                node instanceof DMLStatementNode &&
                (rsn = ((DMLStatementNode) node).getResultSetNode()) != null) {

            ExplainTree tree = buildExplainTree(rsn);
//            List<Pair<String,Integer>> plan = planToString(rsn);
            Map<String, ExplainTree> m=planMap.get();
            m.put(query, tree);

            if (LOG.isInfoEnabled()){
                String treeToString = treeToString(rsn);
                LOG.info(String.format("Plan nodes for query <<\n\t%s\n>>\n%s",
                        query, treeToString));
            }
            if (PLAN_LOG.isTraceEnabled()){
                Gson gson = new GsonBuilder().setPrettyPrinting().create();
                PLAN_LOG.trace(gson.toJson(ImmutableMap.of("query", query, "plan", nodeInfo(rsn, 0))));
            }
        }
        explain = false;
        return node;
    }



    public static Map without(Map m, Object... keys){
        for (Object k: keys){
            m.remove(k);
        }
        return m;
    }

    public static Map prune(Map m){
        List<Object> toPrune = new LinkedList<>();
        for (Map.Entry e: (Set<Map.Entry<Object,Object>>)m.entrySet()){
            Object val = e.getValue();
            if (val == null || (val instanceof List && ((List)val).size() == 0)){
                toPrune.add(e.getKey());
            }
        }
        for (Object k: toPrune){
            m.remove(k);
        }
        return m;
    }

    public static String infoToString(Map<String,Object> info,boolean addSpaces) throws StandardException {
        Map<String,Object> copy = new HashMap<>(info);
        Object clazz = copy.get("class");
        Object results = copy.get("results");
        int level = (Integer)copy.get("level");
        String space;
        if(addSpaces)
           space = Strings.repeat(spaces,level);
        else
            space = "";
            return String.format("%s%s (%s) %s",
                    space,
                    clazz,
                    prune(without(copy, "class", "results", "level", "subqueries", "children")),
                    results != null ? printResults(level+2, (List<Map<String, Object>>) results) : "");
    }

    private static String printResults(int nTimes, List<Map<String, Object>> results)  {
        String indent = Strings.repeat(spaces, nTimes);
        StringBuilder buf = new StringBuilder("\n");
        for (Map<String,Object> row : results) {
            buf.append(indent).append('{');
            for (Map.Entry<String,Object> entry : row.entrySet()) {
                buf.append(entry).append(", ");
            }
            if (buf.length() > 2 && buf.charAt(buf.length()-2) == ',') {
                buf.setLength(buf.length()-2);
                buf.append("}\n");
            }
        }
        if (buf.length() > 0) {
            // remove last '\n'
            buf.setLength(buf.length()-1);
        }
        return buf.toString();
    }

    private ExplainTree buildExplainTree(ResultSetNode rsn) throws StandardException{
        ExplainTree.Builder builder = new ExplainTree.Builder();
        pushExplain(rsn,builder);
        return builder.build();
    }

    private void pushExplain(ResultSetNode rsn,ExplainTree.Builder builder) throws StandardException{
        CostEstimate ce = rsn.getFinalCostEstimate().getBase();
        int rsNum = rsn.getResultSetNumber();

        if(rsn instanceof FromBaseTable){
            FromBaseTable fbt=(FromBaseTable)rsn;
            TableDescriptor tableDescriptor=fbt.getTableDescriptor();
            String tableName = String.format("%s(%s)",tableDescriptor.getName(),tableDescriptor.getHeapConglomerateId());

            ConglomerateDescriptor cd = fbt.getTrulyTheBestAccessPath().getConglomerateDescriptor();
            String indexName = null;
            if (cd.isIndex()) {
                indexName = String.format("%s(%s)", cd.getConglomerateName(), cd.getConglomerateNumber());
            }
            List<String> qualifiers =  Lists.transform(preds(rsn), PredicateUtils.predToString);
            builder.addBaseTable(rsNum,ce,tableName,indexName,qualifiers);
        }else if(rsn instanceof RowResultSetNode){
            builder.pushValuesNode(rsNum,ce);
        } else if(rsn instanceof ProjectRestrictNode){
            pushExplain(((SingleChildResultSetNode)rsn).getChildResult(),builder);
            if(!((ProjectRestrictNode)rsn).nopProjectRestrict()){
                List<String> predicates=Lists.transform(preds(rsn),PredicateUtils.predToString);
                builder.pushProjection(rsNum,ce,predicates);
            }
        }else if(rsn instanceof IndexToBaseRowNode){
            pushExplain(((IndexToBaseRowNode)rsn).getSource(),builder);
            long heapTable=((IndexToBaseRowNode)rsn).getBaseConglomerateDescriptor().getConglomerateNumber();
            builder.pushIndexFetch(rsNum,ce,heapTable);
        }else if(rsn instanceof ScrollInsensitiveResultSetNode){
            pushExplain(((ScrollInsensitiveResultSetNode)rsn).getChildResult(),builder);
        }else if(rsn instanceof JoinNode){
            JoinNode j  = (JoinNode)rsn;
            String joinStrategy = RSUtils.ap(j).getJoinStrategy().getName();
            List<String> joinPreds =  Lists.transform(PredicateUtils.PLtoList(j.joinPredicates),PredicateUtils.predToString);
            ExplainTree.Builder rightBuilder = new ExplainTree.Builder();
            pushExplain(j.getRightResultSet(),rightBuilder);
            //push the left side directly
            pushExplain(j.getLeftResultSet(),builder);

            builder.pushJoin(rsNum,ce,joinStrategy,joinPreds,rightBuilder);
        } else if(rsn instanceof SingleChildResultSetNode){

            pushExplain(((SingleChildResultSetNode)rsn).getChildResult(),builder);
            String nodeName=rsn.getClass().getSimpleName().replace("Node","");
            builder.pushNode(nodeName,rsNum,ce);
        } else if(rsn instanceof TableOperatorNode){
            //we have two tables to work with
            ExplainTree.Builder rightBuilder = new ExplainTree.Builder();
            pushExplain(((TableOperatorNode)rsn).getRightResultSet(),rightBuilder);
            pushExplain(((TableOperatorNode)rsn).getLeftResultSet(),builder);
            builder.pushTableOperator(rsn.getClass().getSimpleName().replace("Node",""),rsNum,ce,rightBuilder);
        }

        //collect subqueries
        List<SubqueryNode> subs = RSUtils.collectExpressionNodes(rsn,SubqueryNode.class);
        if(subs.size()>0){
            ExplainTree.Builder subqueryBuilder = new ExplainTree.Builder();
            for(SubqueryNode subqueryNode : subs){
                pushExplain(subqueryNode.getResultSet(),subqueryBuilder);
                boolean exprSub = subqueryNode.getSubqueryType()==SubqueryNode.EXPRESSION_SUBQUERY;
                boolean corrSub = subqueryNode.hasCorrelatedCRs();
                boolean invariant = subqueryNode.isInvariant();
                builder.pushSubquery(subqueryNode.getResultSet().getResultSetNumber(),exprSub,corrSub,invariant,subqueryBuilder);
                subqueryBuilder.reset(); //reset for the next subquery in the list
            }
        }
    }

    public static Map<String,Object> nodeInfo(final ResultSetNode rsn, final int level) throws StandardException {
        Map<String,Object> info = new HashMap<>();
        CostEstimate co = rsn.getFinalCostEstimate().getBase();
        info.put("class", JoinInfo.className.apply(rsn));
        info.put("n", rsn.getResultSetNumber());
        info.put("level", level);
        info.put("cost", co.prettyString());
        if (Level.TRACE.equals(LOG.getLevel())) {
//        if(LOG.isTraceEnabled()){
          // FIXME: FIND OUT WHY LOG.isTraceEnabled() always returns false

            // we only want to see exec row info when THIS logger is set to trace:
            // com.splicemachine.derby.impl.ast.PlanPrinter=TRACE
            // or
            // call SYSCS_UTIL.SYSCS_SET_LOGGER_LEVEL('com.splicemachine.derby.impl.ast.PlanPrinter', 'TRACE');
            info.put("results", getResultColumnInfo(rsn));
        }
        List<ResultSetNode> children = RSUtils.getChildren(rsn);
        info.put("children", Lists.reverse(Lists.transform(children, new Function<ResultSetNode, Map<String,Object>>() {
            @Override
            public Map<String,Object> apply(ResultSetNode child) {
                try {
                    return nodeInfo(child, level + 1);
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
            }
        })));
        List<SubqueryNode> subs = RSUtils.collectExpressionNodes(rsn, SubqueryNode.class);
        info.put("subqueries", Lists.transform(subs, new Function<SubqueryNode, Map>() {
            @Override
            public Map apply(SubqueryNode subq) {
                try {
                    HashMap<String, Object> subInfo = new HashMap<>();
                    subInfo.put("node", nodeInfo(subq.getResultSet(), 1));
                    subInfo.put("expression?", subq.getSubqueryType() == SubqueryNode.EXPRESSION_SUBQUERY);
                    subInfo.put("correlated?", subq.hasCorrelatedCRs());
                    subInfo.put("invariant?", subq.isInvariant());
                    return subInfo;
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
            }}));
        if (rsn instanceof JoinNode){
            JoinNode j = (JoinNode) rsn;
            info.put("exe", RSUtils.ap(j).getJoinStrategy().getName());
            info.put("preds", Lists.transform(PredicateUtils.PLtoList(j.joinPredicates), PredicateUtils.predToString));
        }
        if (rsn instanceof FromBaseTable){
            FromBaseTable fbt = (FromBaseTable) rsn;
            ConglomerateDescriptor cd = fbt.getTrulyTheBestAccessPath().getConglomerateDescriptor();
            info.put("table", String.format("%s(%s)", fbt.getTableDescriptor().getName(), fbt.getTableDescriptor().getHeapConglomerateId()));
            info.put("quals", Lists.transform(preds(rsn), PredicateUtils.predToString));
            if (cd.isIndex()) {
                info.put("using-index", String.format("%s(%s)", cd.getConglomerateName(), cd.getConglomerateNumber()));
            }

        }
        if (rsn instanceof IndexToBaseRowNode){
            IndexToBaseRowNode idx = (IndexToBaseRowNode) rsn;
            //info.put("name", idx.getName());
        }
        if (rsn instanceof ProjectRestrictNode){
            info.put("quals", Lists.transform(preds(rsn), PredicateUtils.predToString));
        }
        if (rsn instanceof WindowResultSetNode) {
            info.put("functions",((WindowResultSetNode)rsn).getFunctionNames());
        }
        return info;
    }

    public static List<Map<String, Object>> getResultColumnInfo(ResultSetNode rsn) throws StandardException {
        List<Map<String, Object>> resultColumns = new ArrayList<Map<String, Object>>();
        ResultColumnList resultColumnList = rsn.getResultColumns();
        if (resultColumnList != null && resultColumnList.size() > 0) {

            for (ResultColumn resultColumn : resultColumnList) {
                Map<String, Object> columnInfo = new LinkedHashMap<String, Object>();
                if (resultColumn != null) {
                    columnInfo.put("column", resultColumn.getName());
                    columnInfo.put("position", resultColumn.getColumnPosition());
                    columnInfo.put("type", resultColumn.getType());
                    ValueNode exp = resultColumn.getExpression();
                    if (exp != null) {
                        String columnName = exp.getColumnName();
                        if (columnName != null) {
                            columnInfo.put("exp", columnName);
                        }
                        String src = "null";
                        if (exp instanceof VirtualColumnNode) {
                            ResultColumn rc = ((VirtualColumnNode)exp).getSourceColumn();
                            if (rc != null) {
                                src = rc.getName()+"(from RS "+rc.getResultSetNumber()+")";
                            }
                        } else if (exp instanceof ColumnReference) {
                            ResultColumn rc = ((ColumnReference)exp).getSource();
                            if (rc != null) {
                                src = rc.getName()+"(from RS "+rc.getResultSetNumber()+")";
                            }
                        } else if (exp instanceof ConstantNode) {
                            DataValueDescriptor dvd = ((ConstantNode)exp).getValue();
                            if (dvd != null) {
                                src = dvd.getString();
                            }
                        }
                        columnInfo.put("src", src);
                        columnInfo.put("exp node", exp.getClass().getSimpleName());
                    }
                }
                resultColumns.add(columnInfo);
            }
        }
        return resultColumns;
    }

    public static List<Map<String,Object>> linearizeNodeInfoTree(Map<String, Object> info)
            throws StandardException {
        List<Map<String,Object>> children = (List<Map<String,Object>>)info.get("children");
        List<Map<String,Object>> nodes = new LinkedList<Map<String, Object>>();
        nodes.add(info);
        for (Map<String,Object> child : Lists.reverse(children)) {
            nodes.addAll(linearizeNodeInfoTree(child));
        }
        return nodes;
    }

    public static String treeToString(Map<String,Object> nodeInfo) throws StandardException {
        List<Pair<Integer,Map>> subs = new LinkedList<>();
        StringBuilder sb = new StringBuilder();
        List<Map<String,Object>> nodes = linearizeNodeInfoTree(nodeInfo);
        for (Map<String,Object> node: nodes){
            List<Map> subqs = (List<Map>)node.get("subqueries");
            if (subqs != null){
                for (Map subInfo: subqs){
                    subs.add(new Pair<>((Integer)node.get("n"), subInfo));
                }
            }
            sb.append(infoToString(node,true));
            sb.append("\n");
        }
        for (Pair<Integer,Map> sub: subs){
            Map subInfo = sub.getSecond();
            Map<String,Object> subqInfoNode = (Map<String,Object>)subInfo.get("node");
            sb.append(subqueryToString(subInfo,subqInfoNode));
            sb.append(treeToString(subqInfoNode));
        }
        return sb.toString();
    }

    private static String subqueryToString(Map subInfo,Map<String, Object> subqInfoNode){
        return String.format("\nSubquery n=%s: expression?=%s, invariant?=%s, correlated?=%s\n",
                subqInfoNode.get("n"),subInfo.get("expression?"),
                subInfo.get("invariant?"),subInfo.get("correlated?"));
    }

    public static String treeToString(ResultSetNode rsn, int initLevel) throws StandardException {
        return treeToString(nodeInfo(rsn, initLevel));
    }

    public static String treeToString(ResultSetNode rsn) throws StandardException {
        return treeToString(rsn,0);
    }

    private static List<Predicate> preds(ResultSetNode t) throws StandardException {
        PredicateList pl;
        if(t instanceof FromBaseTable)
            pl = RSUtils.getPreds((FromBaseTable)t);
        else if(t instanceof ProjectRestrictNode)
            pl = RSUtils.getPreds((ProjectRestrictNode)t);
        else if(t instanceof IndexToBaseRowNode)
            pl = RSUtils.getPreds((IndexToBaseRowNode)t);
        else
            throw new IllegalArgumentException("Programmer error: Unable to determine class with predicates:"+t.getClass());

        return PredicateUtils.PLtoList(pl);
    }

    private static List<Pair<String,Integer>> planToString(ResultSetNode rsn) throws StandardException{
        Map<String, Object> nodeInfo=nodeInfo(rsn,0);
        List<Pair<String,Integer>> flattenedPlanMap = new LinkedList<>();
//        Map<Integer,String> planMap = new HashMap<>();
        pushPlanInfo(nodeInfo,flattenedPlanMap);
        return flattenedPlanMap;
    }


    private static void pushPlanInfo(Map<String, Object> nodeInfo,List<Pair<String,Integer>> planMap) throws StandardException{
        @SuppressWarnings("unchecked") List<Map<String,Object>> children = (List<Map<String,Object>>)nodeInfo.get("children");
        String thisNodeInfo = infoToString(nodeInfo,false);
        Integer level = (Integer)nodeInfo.get("level");
        planMap.add(new Pair<>(thisNodeInfo,level));
        for(Map<String,Object> child:children){
            pushPlanInfo(child,planMap);
        }

        if(!nodeInfo.containsKey("subqueries")) return; //nothing to work with
        @SuppressWarnings("unchecked") List<Map<String,Object>> subqueries = (List<Map<String,Object>>)nodeInfo.get("subqueries");
        for(Map<String,Object> subquery:subqueries){
            Map<String,Object> subqueryNodeInfo = (Map<String,Object>)subquery.get("node");
            pushPlanInfo(subqueryNodeInfo,planMap);
            String subqueryInfo = subqueryToString(subquery,subqueryNodeInfo);
            planMap.add(new Pair<>(subqueryInfo,level));
        }
    }

}
