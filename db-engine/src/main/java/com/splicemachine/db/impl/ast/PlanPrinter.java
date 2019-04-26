/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.ast;

import org.spark_project.guava.base.Function;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.compile.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Strings;
import org.spark_project.guava.collect.Iterators;
import org.spark_project.guava.collect.Lists;
import java.util.*;


/**
 * @author P Trolard
 *         Date: 30/09/2013
 */
public class PlanPrinter extends AbstractSpliceVisitor {
    public static Logger LOG = Logger.getLogger(PlanPrinter.class);
    public static final String spaces = "  ";
    private boolean explain = false;
    public static ThreadLocal<Map<String,Collection<QueryTreeNode>>> planMap = new ThreadLocal<Map<String,Collection<QueryTreeNode>>>(){
        @Override
        protected Map<String,Collection<QueryTreeNode>> initialValue(){
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
        DMLStatementNode rsn;
        if ((explain || LOG.isDebugEnabled()) &&
                node instanceof DMLStatementNode &&
                (((DMLStatementNode) node).getResultSetNode()) != null) {
            rsn = (DMLStatementNode) node;
            List<QueryTreeNode> orderedNodes = new ArrayList<>();
            rsn.buildTree(orderedNodes,0);
            Map<String, Collection<QueryTreeNode>> m=planMap.get();
            m.put(query,orderedNodes);
            if (LOG.isDebugEnabled()){
                CompilerContext.DataSetProcessorType currentType = rsn.getLanguageConnectionContext().getDataSetProcessorType();
                boolean useSpark = (currentType == CompilerContext.DataSetProcessorType.SPARK);
                if (currentType == CompilerContext.DataSetProcessorType.FORCED_SPARK)
                    useSpark = true;
                else if (currentType == CompilerContext.DataSetProcessorType.FORCED_CONTROL)
                    useSpark = false;
                else {
                    // query may have provided hint on useSpark=true/false
                    CompilerContext.DataSetProcessorType  queryForcedType = queryHintedForcedType(orderedNodes);
                    if (queryForcedType != null) {
                        if (queryForcedType == CompilerContext.DataSetProcessorType.FORCED_SPARK)
                            useSpark = true;
                        else if (queryForcedType == CompilerContext.DataSetProcessorType.FORCED_CONTROL)
                            useSpark = false;
                    }
                    else if (!useSpark)
                        useSpark = PlanPrinter.shouldUseSpark(orderedNodes, true);
                }

                Iterator<String> nodes = planToIterator(orderedNodes, useSpark);
                StringBuilder sb = new StringBuilder();
                while (nodes.hasNext())
                    sb.append(nodes.next()).append("\n");
                LOG.info(String.format("Plan nodes for query <<\n\t%s\n>>%s\n",
                        query,sb.toString()));
            }
        }
        explain = false;
        return node;
    }

    public static boolean shouldUseSpark(Collection<QueryTreeNode> opPlanMap, boolean needDetermineSpark) {
        boolean useSpark = false;
        for (QueryTreeNode node : opPlanMap) {
            if (node instanceof FromBaseTable) {
                // in case we are calling this function before the code generation phase, detemineSpark() hasn't been called,
                // and the dataSetProcessorType in the FromBaseTable node may not be properly set yet, so we need to call this
                // function before using dataSetProcessorType
                if (needDetermineSpark)
                    ((FromBaseTable) node).determineSpark();
                CompilerContext.DataSetProcessorType dataSetProcessorType
                        = ((FromBaseTable) node).getDataSetProcessorType();
                if (dataSetProcessorType == CompilerContext.DataSetProcessorType.FORCED_SPARK ||
                        dataSetProcessorType == CompilerContext.DataSetProcessorType.SPARK) {
                    useSpark = true;
                    break;
                }
            }
        }

        return useSpark;
    }

    public static CompilerContext.DataSetProcessorType  queryHintedForcedType(Collection<QueryTreeNode> opPlanMap) {
        for (QueryTreeNode node : opPlanMap) {
            if (node instanceof FromBaseTable) {
                CompilerContext.DataSetProcessorType hintType = ((FromBaseTable) node).getDataSetProcessorType();
                if (hintType  == CompilerContext.DataSetProcessorType.FORCED_SPARK)
                    return CompilerContext.DataSetProcessorType.FORCED_SPARK;
                else if (hintType == CompilerContext.DataSetProcessorType.FORCED_CONTROL)
                    return CompilerContext.DataSetProcessorType.FORCED_CONTROL;
            }
        }
        return null;
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
            if (val == null || (val instanceof List && ((List) val).isEmpty())){
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
           space = Strings.repeat(spaces, level);
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

    public static Map<String,Object> nodeInfo(final ResultSetNode rsn, final int level) throws StandardException {
        Map<String,Object> info = new HashMap<>();
        CostEstimate co = rsn.getFinalCostEstimate(false).getBase();
        info.put("class", JoinInfo.className.apply(rsn));
        info.put("n", rsn.getResultSetNumber());
        info.put("level", level);
        info.put("cost", co.prettyProcessingString());
        if (Level.TRACE.equals(LOG.getLevel())) {
//        if(LOG.isTraceEnabled()){
          // FIXME: FIND OUT WHY LOG.isTraceEnabled() always returns false

            // we only want to see exec row info when THIS logger is set to trace:
            // com.splicemachine.db.impl.ast.PlanPrinter=TRACE
            // or
            // call SYSCS_UTIL.SYSCS_SET_LOGGER_LEVEL('com.splicemachine.db.impl.ast.PlanPrinter', 'TRACE');
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
            }
        }));
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
        List<Map<String, Object>> resultColumns = new ArrayList<>();
        ResultColumnList resultColumnList = rsn.getResultColumns();
        if (resultColumnList != null && !resultColumnList.isEmpty()) {

            for (ResultColumn resultColumn : resultColumnList) {
                Map<String, Object> columnInfo = new LinkedHashMap<>();
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
        List<Map<String,Object>> nodes = new LinkedList<>();
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
                    subs.add(Pair.of((Integer)node.get("n"), subInfo));
                }
            }
            sb.append(infoToString(node,true));
            sb.append("\n");
        }
        for (Pair<Integer,Map> sub: subs){
            Map subInfo = sub.getRight();
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
        planMap.add(Pair.of(thisNodeInfo,level));
        for(Map<String,Object> child:children){
            pushPlanInfo(child,planMap);
        }

        if(!nodeInfo.containsKey("subqueries")) return; //nothing to work with
        @SuppressWarnings("unchecked") List<Map<String,Object>> subqueries = (List<Map<String,Object>>)nodeInfo.get("subqueries");
        for(Map<String,Object> subquery:subqueries){
            Map<String,Object> subqueryNodeInfo = (Map<String,Object>)subquery.get("node");
            pushPlanInfo(subqueryNodeInfo,planMap);
            String subqueryInfo = subqueryToString(subquery,subqueryNodeInfo);
            planMap.add(Pair.of(subqueryInfo,level));
        }
    }

    public static Iterator<String> planToIterator(final Collection<QueryTreeNode> orderedNodes, final boolean useSpark) throws StandardException {
        return Iterators.transform(orderedNodes.iterator(), new Function<QueryTreeNode, String>() {
            int i = 0;

            @Override
            public String apply(QueryTreeNode queryTreeNode) {
                try {
                    if ((queryTreeNode instanceof UnionNode) && ((UnionNode) queryTreeNode).getIsRecursive()) {
                        ((UnionNode) queryTreeNode).setStepNumInExplain(orderedNodes.size() - i);
                    }
                    return queryTreeNode.printExplainInformation(orderedNodes.size(), i, useSpark, true);
                } catch (StandardException se) {
                    throw new RuntimeException(se);
                } finally {
                    i++;
                }
            }
        });
    }

}
