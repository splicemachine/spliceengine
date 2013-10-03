package com.splicemachine.derby.impl.ast;

import com.google.common.base.*;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.*;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.impl.sql.compile.*;
import org.apache.derby.impl.sql.compile.Predicate;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.*;

/**
 * @author P Trolard
 *         Date: 09/09/2013
 */
public class JoinSelector extends AbstractSpliceVisitor {

    private static Logger LOG = Logger.getLogger(JoinSelector.class);

    // JoinStrategies are stateless so can be reused
    public final static BroadcastJoinStrategy BCAST  = new BroadcastJoinStrategy();
    public final static MergeSortJoinStrategy MSJ    = new MergeSortJoinStrategy();
    public final static NestedLoopJoinStrategy NLJ   = new NestedLoopJoinStrategy();

    @Override
    public QueryTreeNode visit(JoinNode j) throws StandardException {
        try {
            LOG.info(String.format("JoinSelector visiting JoinNode for query << %s >>", query));

            JoinInfo info = joinInfo(j);
            JoinStrategy chosen = chooseStrategy(info);
            if (!info.strategy.getClass().equals(chosen.getClass())) {
                LOG.info(String.format("Strategy changed from %s to %s for join %s",
                        info.strategy, chosen, info));
                return withStrategy(j, chosen);
            } else {
                return j;
            }

        } catch (RuntimeException re) {
            LOG.info(String.format("Exception choosing join strategy for %s, bailing", query), re);
            return j;
        }
    }

    public static JoinStrategy chooseStrategy(JoinInfo info) throws StandardException {
        // If reasons to bail present, return Derby's strategy
        if (info.userSuppliedStrategy ||
                info.isSystemTable ||
                info.rightLeaves.size() != 1 ||
                info.rightLeaves.get(0).getClass() != FromBaseTable.class ||
                info.hasRightIndex){
            LOG.debug("--> BAILING");
            return info.strategy;
        }
        // If cross-join or non-equijoin, use NLJ
        if (info.joinPredicates.size() == 0 ||
                !info.isEquiJoin){
            return NLJ;
        }
        // If right join column is PK, use NLJ
        if (info.rightEquiJoinColIsPK){
            return NLJ;
        }

        return MSJ;
    }

    public static JoinNode withStrategy(JoinNode j, JoinStrategy s) throws StandardException {
        LOG.debug(String.format("--> SETTING STRATEGY %s", s));
        ap(j).setJoinStrategy(s);
        // With new strategy set, regenerate access path
        j.getRightResultSet().changeAccessPath();
        return j;
    }

    public static JoinInfo joinInfo(JoinNode j) throws StandardException {
        List<ResultSetNode> rightNodes = getSelfAndChildren(j.getRightResultSet());
        List<ResultSetNode> rightLeaves = getLeafNodes(j.getRightResultSet());

        // Predicates
        List<Predicate> preds = getRightPreds(j);
        List<Predicate> joinPreds = new ArrayList<Predicate>(preds.size());
        List<Predicate> otherPreds = new ArrayList<Predicate>(preds.size());

        for (Predicate p: preds){
            if (p.isJoinPredicate()){
                joinPreds.add(p);
            } else {
                otherPreds.add(p);
            }
        }

        // Index?
        boolean hasRightIndex = containsClass(rightNodes, IndexToBaseRowNode.class);

        boolean userSupplied = nodesContainStrategyHint(j, getSelfAndChildren(j));

        ConglomerateDescriptor cd = ap(j).getConglomerateDescriptor();
        boolean isSystemTable = cd != null &&
                                    cd.getSchemaID().toString()
                                        .equals(SchemaDescriptor.SYSTEM_SCHEMA_UUID);

        return new JoinInfo(strategy(j),
                            userSupplied,
                            isSystemTable,
                            isEquijoin(joinPreds),
                            false,
                            hasRightIndex,
                            joinPreds,
                            otherPreds,
                            rightNodes,
                            rightLeaves);
    }

    public static boolean nodesContainStrategyHint(JoinNode j, List<ResultSetNode> rsns){
        for (ResultSetNode rsn: rsns){
            Properties props = ((Optimizable)rsn).getProperties();
            if (props != null &&
                    props.getProperty("joinStrategy") != null &&
                    (j == rsn ||
                            j.getReferencedTableMap().contains(rsn.getReferencedTableMap()))){
                return true;
            }
        }
        return false;
    }

    public static List<ResultSetNode> getSelfAndChildren(ResultSetNode rsn) throws StandardException {
        return ColumnUtils.collectNodes(rsn, ResultSetNode.class);
    }

    public static Set binaryRSNs = ImmutableSet.of(JoinNode.class, HalfOuterJoinNode.class, UnionNode.class, IntersectOrExceptNode.class);
    public static Set leafRSNs = ImmutableSet.of(FromBaseTable.class, RowResultSetNode.class);

    /**
     * If rsn subtree contains a node with 2 children, return the node above
     * it, else return the leaf node
     */
    public static ResultSetNode getLastNonBinaryNode(ResultSetNode rsn) throws StandardException {
        List<ResultSetNode> rsns = getSelfAndChildren(rsn);
        for (List<ResultSetNode> pair: Iterables.paddedPartition(rsns, 2)){
            if (pair.get(1) != null && binaryRSNs.contains(pair.get(1).getClass())){
                return pair.get(0);
            }
        }
        return rsns.get(rsns.size() - 1);
    }

    /**
     * Returns the leaves for a query plan subtree
     */
    public static List<ResultSetNode> getLeafNodes(ResultSetNode rsn)
            throws StandardException {
        List<ResultSetNode> rsns = getSelfAndChildren(rsn);
        List<ResultSetNode> leaves = new LinkedList<ResultSetNode>();
        for (ResultSetNode r: rsns){
            if (leafRSNs.contains(r.getClass())){
                leaves.add(r);
            }
        }
        return leaves;
    }

    public static List<Predicate> getRightPreds(JoinNode j) throws StandardException {
        return preds(getLastNonBinaryNode(j.getRightResultSet()));
    }

    public static List<Predicate> preds(ResultSetNode t) throws StandardException {
        PredicateList pl = t instanceof FromBaseTable ?
                                getPreds((FromBaseTable) t) : getPreds((ProjectRestrictNode) t);
        List<Predicate> preds = new ArrayList<Predicate>(pl.size());
        for (int i = 0, s = pl.size(); i < s; i++){
            OptimizablePredicate p = pl.getOptPredicate(i);
            preds.add((Predicate) p);
        }
        return preds;
    }

    public static PredicateList getPreds(FromBaseTable t) throws StandardException {
        PredicateList pl = new PredicateList();
        t.pullOptPredicates(pl);
        for (int i = 0, s = pl.size(); i < s; i++){
            OptimizablePredicate p = pl.getOptPredicate(i);
            t.pushOptPredicate(p);
        }
        return pl;
    }

    public static PredicateList getPreds(ProjectRestrictNode pr) throws StandardException {
        return pr.restrictionList;
    }

    public static boolean isEquijoin(List<Predicate> preds) throws StandardException {
        for (Predicate p: preds){
            if (p.isJoinPredicate() &&
                    p.getAndNode().getLeftOperand().isBinaryEqualsOperatorNode()){
                return true;
            }
        }
        return false;
    }

    public static AccessPath ap(JoinNode j){
         return ((Optimizable) j.getRightResultSet())
                 .getTrulyTheBestAccessPath();
    }

    public static JoinStrategy strategy(JoinNode j){
        return ap(j).getJoinStrategy();
    }

    public static boolean containsClass(List<?> list, Class clazz){
        for (Object o: list){
            if (clazz.isInstance(o)){
                return true;
            }
        }
        return false;
    }

    public static boolean rightColIsPK(JoinNode j, FromBaseTable fbt, List<Predicate> equiPreds){
        return false;
    }

}
