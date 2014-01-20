package com.splicemachine.derby.impl.ast;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.Visitable;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.impl.sql.compile.*;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
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
        List<SubqueryNode> subs = RSUtils.collectExpressionNodes(rsn, SubqueryNode.class);
        info.put("subqueries", Lists.transform(subs, new Function<SubqueryNode, Map>() {
            @Override
            public Map apply(SubqueryNode subq) {
                try {
                    HashMap<String, Object> subInfo = new HashMap<String, Object>();
                    subInfo.put("node", subq);
                    subInfo.put("expression?", subq.getSubqueryType() ==
                            SubqueryNode.EXPRESSION_SUBQUERY);
                    subInfo.put("correlated?", subq.hasCorrelatedCRs());
                    subInfo.put("invariant?", subq.isInvariant());
                    return subInfo;
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
            }}));
        if (rsn instanceof JoinNode){
            JoinNode j = (JoinNode) rsn;
            info.put("exe", JoinSelector.strategy(j).getName());
            info.put("preds", Lists.transform(PredicateUtils.PLtoList(j.joinPredicates),
                                                PredicateUtils.predToString));
        }
        if (rsn instanceof FromBaseTable){
            FromBaseTable fbt = (FromBaseTable) rsn;
            ConglomerateDescriptor cd = fbt.getTrulyTheBestAccessPath().getConglomerateDescriptor();
            info.put("table", String.format("%s,%s",
                                fbt.getTableDescriptor().getName(),
                                fbt.getTableDescriptor().getHeapConglomerateId()));
            info.put("quals", Lists.transform(JoinSelector.preds(rsn),
                                                PredicateUtils.predToString));
            if (cd.isIndex()) {
                info.put("using-index", String.format("%s,%s", cd.getConglomerateName(),
                                                                cd.getConglomerateNumber()));
            }

        }
        if (rsn instanceof IndexToBaseRowNode){
            IndexToBaseRowNode idx = (IndexToBaseRowNode) rsn;
            //info.put("name", idx.getName());
        }
        if (rsn instanceof ProjectRestrictNode){
            info.put("quals", Lists.transform(JoinSelector.preds(rsn),
                                                PredicateUtils.predToString));
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
        List<Pair<Integer,Map>> subs = new LinkedList<Pair<Integer, Map>>();
        StringBuilder sb = new StringBuilder();
        List<Map<String,Object>> nodes = treeToInfoNodes(initLevel, rsn);
        for (Map<String,Object> node: nodes){
            List<Map> subqs = (List<Map>)node.get("subqueries");
            if (subqs != null){
                for (Map subInfo: subqs){
                    subs.add(new Pair<Integer,Map>((Integer)node.get("n"), subInfo));
                }
            }
            sb.append(infoToString(node));
            sb.append("\n");
        }
        for (Pair<Integer,Map> sub: subs){
            Map subInfo = sub.getSecond();
            SubqueryNode subq = (SubqueryNode)subInfo.get("node");
            sb.append(String.format(
                    "\nSubquery n=%s: expression?=%s, invariant?=%s, correlated?=%s\n",
                    subq.getResultSet().getResultSetNumber(), subInfo.get("expression?"),
                    subInfo.get("invariant?"), subInfo.get("correlated?")));
            sb.append(treeToString(subq.getResultSet(), 1));
        }
        return sb.toString();
    }

    public static String treeToString(ResultSetNode rsn) throws StandardException {
        return treeToString(rsn, 0);
    }
}
