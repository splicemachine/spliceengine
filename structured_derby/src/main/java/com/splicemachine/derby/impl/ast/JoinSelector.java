package com.splicemachine.derby.impl.ast;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.*;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.impl.sql.compile.*;
import org.apache.derby.impl.sql.compile.Predicate;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
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
            JoinInfo info = joinInfo(j);
            JoinStrategy chosen = chooseStrategy(info);
            if (!info.strategy.getClass().equals(chosen.getClass())) {
                if (LOG.isInfoEnabled()){
                    LOG.info(String.format("Strategy changed from %s to %s for join %s",
                                info.strategy, chosen, info));
                }
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
        List<ResultSetNode> rightNodes = RSUtils.getSelfAndDescendants(j.getRightResultSet());
        List<ResultSetNode> rightLeaves = RSUtils.getLeafNodes(j.getRightResultSet());

        // Predicates
        List<Predicate> preds = Lists.newLinkedList(getRightPreds(j));
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

        boolean userSupplied = nodesContainStrategyHint(j, RSUtils.getSelfAndDescendants(j));

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

    public static Iterable<Predicate> getRightPreds(JoinNode j) throws StandardException {
        Iterable<ResultSetNode> rightsUntilBinary = Iterables.filter(
                RSUtils.collectNodesUntil(j.getRightResultSet(), ResultSetNode.class,
                        RSUtils.isBinaryRSN),
                RSUtils.rsnHasPreds);
        return Iterables.concat(
                Iterables.transform(rightsUntilBinary,
                        new Function<ResultSetNode, List<Predicate>>() {
                            @Override
                            public List<Predicate> apply(ResultSetNode rsn) {
                                try {
                                    return preds(rsn);
                                } catch (StandardException se){
                                    throw new RuntimeException(se);
                                }
                            }
                        }));
    }

    public static List<Predicate> preds(ResultSetNode t) throws StandardException {
        PredicateList pl = t instanceof FromBaseTable ?
                                RSUtils.getPreds((FromBaseTable) t) : RSUtils.getPreds((ProjectRestrictNode) t);
        return PredicateUtils.PLtoList(pl);
    }

    public static boolean isEquijoin(List<Predicate> preds) throws StandardException {
        for (Predicate p: preds){
            if (PredicateUtils.isEquiJoinPred.apply(p)){
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
