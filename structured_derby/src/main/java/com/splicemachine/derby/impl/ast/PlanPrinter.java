package com.splicemachine.derby.impl.ast;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.impl.sql.compile.*;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.util.*;


/**
 * @author P Trolard
 *         Date: 30/09/2013
 */
public class PlanPrinter extends AbstractSpliceVisitor {

    public static Logger LOG = Logger.getLogger(PlanPrinter.class);

    public static final String spaces = "  ";

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
    public Visitable defaultVisit(Visitable node) throws StandardException {
        ResultSetNode rsn;
        if (LOG.isInfoEnabled() &&
                node instanceof DMLStatementNode &&
                (rsn = ((DMLStatementNode) node).getResultSetNode()) != null) {
            LOG.info(String.format("Plan nodes for query <<\n\t%s\n>>\n%s",
                    query, treeToString(rsn)));
        }
        return node;
    }

    public static Map without(Map m, Object... keys){
        for (Object k: keys){
            m.remove(k);
        }
        return m;
    }

    public static Map prune(Map m){
        List<Object> toPrune = new LinkedList<Object>();
        for (Map.Entry e: (Set<Map.Entry<Object,Object>>)m.entrySet()){
            Object val = e.getValue();
            if (val == null ||
                    val instanceof List &&
                            ((List)val).size() == 0){
                toPrune.add(e.getKey());
            }
        }
        for (Object k: toPrune){
            m.remove(k);
        }
        return m;
    }

    public static String infoToString(Map<String,Object> info)
            throws StandardException {
        Map<String,Object> copy = new HashMap<String, Object>(info);
        Object clazz = copy.get("class");
        int level = (Integer)copy.get("level");
        return String.format("%s%s (%s)",
                Strings.repeat(spaces, level),
                clazz,
                prune(without(copy, "class", "level", "subqueries")));
    }

    public static Map<String,Object> nodeInfo(ResultSetNode rsn, int level)
            throws StandardException {
        Map<String,Object> info = new HashMap<String, Object>();
        info.put("class", JoinInfo.className.apply(rsn));
        info.put("n", rsn.getResultSetNumber());
        info.put("level", level);
        if (rsn instanceof JoinNode){
            JoinNode j = (JoinNode) rsn;
            info.put("exe", JoinSelector.strategy(j).getName());
            info.put("preds", Lists.transform(PredicateUtils.PLtoList(j.joinPredicates), PredicateUtils.predToString));
        }
        if (rsn instanceof FromBaseTable){
            FromBaseTable fbt = (FromBaseTable) rsn;
            info.put("table", String.format("%s,%s",
                                fbt.getTableDescriptor().getName(),
                                fbt.getTableDescriptor().getHeapConglomerateId()));
            info.put("quals", Lists.transform(JoinSelector.preds(rsn), PredicateUtils.predToString));
        }
        if (rsn instanceof IndexToBaseRowNode){
            IndexToBaseRowNode idx = (IndexToBaseRowNode) rsn;
            //info.put("name", idx.getName());
        }
        if (rsn instanceof ProjectRestrictNode){
            info.put("quals", Lists.transform(JoinSelector.preds(rsn), PredicateUtils.predToString));
            info.put("subqueries", RSUtils.collectNodes(rsn, SubqueryNode.class));
        }
        return info;
    }

    public static List<Map<String,Object>> treeToInfoNodes(int level, ResultSetNode node)
            throws StandardException {
        List<ResultSetNode> children = RSUtils.getChildren(node);
        List<Map<String,Object>> nodes = new LinkedList<Map<String, Object>>();
        nodes.add(nodeInfo(node, level));
        for (ResultSetNode child : Lists.reverse(children)) {
            nodes.addAll(treeToInfoNodes(level + 1, child));
        }
        return nodes;
    }

    public static String treeToString(ResultSetNode rsn, int initLevel)
            throws StandardException {
        List<Pair<Integer,SubqueryNode>> subs = new LinkedList<Pair<Integer, SubqueryNode>>();
        StringBuilder sb = new StringBuilder();
        List<Map<String,Object>> nodes = treeToInfoNodes(initLevel, rsn);
        for (Map<String,Object> node: nodes){
            List<SubqueryNode> subqs = (List<SubqueryNode>)node.get("subqueries");
            if (subqs != null){
                for (SubqueryNode subq: subqs){
                    subs.add(new Pair<Integer,SubqueryNode>((Integer)node.get("n"), subq));
                }
            }
            sb.append(infoToString(node));
            sb.append("\n");
        }
        for (Pair<Integer,SubqueryNode> sub: subs){
            sb.append(String.format("\nSubquery from node n=%s:\n", sub.getFirst()));
            sb.append(treeToString(sub.getSecond().getResultSet(), 1));
        }
        return sb.toString();
    }

    public static String treeToString(ResultSetNode rsn) throws StandardException {
        return treeToString(rsn, 0);
    }
}
