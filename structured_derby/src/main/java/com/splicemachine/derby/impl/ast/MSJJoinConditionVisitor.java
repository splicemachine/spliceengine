package com.splicemachine.derby.impl.ast;


import com.google.common.base.*;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.impl.sql.compile.*;
import org.apache.derby.impl.sql.compile.Predicate;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.util.*;


/**
 * This visitor moves join predicates from joined tables to the join nodes themselves for
 * hashable joins. During optimization, Derby pushes down the predicates to FromBaseTable
 * (& ProjectRestrict) nodes, assuming that a table on one side of the join will have access
 * to the "current row" of a table on the other side at query execution time, an assumption
 * which we know to be incorrect for MSJ. This visitor reverses the pushdown.
 *
 * User: pjt
 * Date: 7/8/13
 */

public class MSJJoinConditionVisitor extends AbstractSpliceVisitor {

    private static Logger LOG = Logger.getLogger(MSJJoinConditionVisitor.class);

    @Override
    public JoinNode visit(JoinNode j) throws StandardException {
        AccessPath ap = ((Optimizable) j.getRightResultSet()).getTrulyTheBestAccessPath();
        return RSUtils.isHashableJoin(ap) ?
                pullUpPreds(j)  : j;
    }

    @Override
    public JoinNode visit(HalfOuterJoinNode j) throws StandardException {
        return visit((JoinNode)j);
    }

    public JoinNode pullUpPreds(JoinNode j) throws StandardException {
        List<Predicate> toPullUp = new LinkedList<Predicate>();

        // Collect PRs, FBTs until a binary node (Union, Join) found, or end
        Iterable<ResultSetNode> rightsUntilBinary = Iterables.filter(
                RSUtils.collectNodes(j.getRightResultSet(), ResultSetNode.class,
                        RSUtils.isBinaryRSN),
                RSUtils.rsnHasPreds);

        com.google.common.base.Predicate<Predicate> joinScoped = evalableAtNode(j);

        for (ResultSetNode rsn: rightsUntilBinary) {
            // Encode whether to pull up predicate to join:
            //  when can't evaluate on node but can evaluate at join
            com.google.common.base.Predicate<Predicate> shouldPull =
                    Predicates.and(Predicates.not(evalableAtNode(rsn)), joinScoped);
            toPullUp.addAll(rsn instanceof ProjectRestrictNode ?
                                pullPredsFromPR((ProjectRestrictNode)rsn, shouldPull) :
                                    pullPredsFromTable((FromBaseTable)rsn, shouldPull));
        }

        for (Predicate p: toPullUp){
            p = updatePredColRefsToJoin(p, j);
            j.addOptPredicate(p);
            LOG.debug(String.format("Added pred %s to Join=%s.",
                    PredicateUtils.predToString.apply(p), j.getResultSetNumber()));
        }

        return j;
    }

    public List<Predicate> pullPredsFromPR(ProjectRestrictNode pr,
                                           com.google.common.base.Predicate<Predicate> shouldPull)
            throws StandardException {
        List<Predicate> pulled = new LinkedList<Predicate>();
        if (pr.restrictionList != null) {
            for (int i = pr.restrictionList.size() - 1; i >= 0; i--) {
                Predicate p = (Predicate)pr.restrictionList.getOptPredicate(i);
                if (shouldPull.apply(p)) {
                    pulled.add(p);
                    LOG.debug(String.format("Pulled pred %s from PR=%s",
                            PredicateUtils.predToString.apply(p), pr.getResultSetNumber()));
                    pr.restrictionList.removeOptPredicate(i);
                }
            }
        }
        return pulled;
    }

    public List<Predicate> pullPredsFromTable(FromBaseTable t,
                                              com.google.common.base.Predicate<Predicate> shouldPull)
            throws StandardException {
        List<Predicate> pulled = new LinkedList<Predicate>();
        PredicateList pl = new PredicateList();
        t.pullOptPredicates(pl);
        for (int i = 0, s = pl.size(); i < s; i++) {
            Predicate p = (Predicate)pl.getOptPredicate(i);
            if (shouldPull.apply(p)) {
                pulled.add(p);
                LOG.debug(String.format("Pulled pred %s from Table=%s",
                        PredicateUtils.predToString.apply((Predicate) p), t.getResultSetNumber()));
            } else {
                t.pushOptPredicate(p);
            }
        }
        return pulled;
    }


    /**
     * Return the set of ResultSetNode numbers referred to by column references in p
     */
    public static Set<Integer> resultSetRefs(Predicate p) throws StandardException {
        return Sets.newHashSet(
                Lists.transform(RSUtils.collectNodes(p, ColumnReference.class),
                    new Function<ColumnReference, Integer>() {
                        @Override
                        public Integer apply(ColumnReference cr) {
                            return ColumnUtils.RSCoordinate(cr.getSource()).getFirst();
                        }}));
    }


    /**
     * Returns a fn that returns true if a Predicate can be evaluated at the node rsn
     */
    public static com.google.common.base.Predicate<Predicate> evalableAtNode(final ResultSetNode rsn)
            throws StandardException {
        final Set<Integer> rsns = Sets.newHashSet(Lists.transform(RSUtils.getSelfAndDescendants(rsn), RSUtils.rsNum));
        return new com.google.common.base.Predicate<Predicate>() {
            @Override
            public boolean apply(Predicate p) {
                try {
                    return rsns.containsAll(resultSetRefs(p));
                } catch (StandardException se){
                    throw new RuntimeException(se);
                }
            }
        };
    }

    /**
     * Rewrites column references in a Predicate to point to ResultColumns from the passed join node.
     */
    public Predicate updatePredColRefsToJoin(Predicate p, JoinNode j)
            throws StandardException
    {
        ResultColumnList rcl = j.getResultColumns();
        Map<Pair<Integer,Integer>, ResultColumn> chain = ColumnUtils.rsnChainMap(rcl);
        List<ColumnReference> predCRs = RSUtils.collectNodes(p, ColumnReference.class);
        for (ColumnReference cr: predCRs){
            ResultColumn rc = cr.getSource();
            Pair<Integer,Integer> rsnAndCol = ColumnUtils.RSCoordinate(rc);
            if (chain.containsKey(rsnAndCol)){
                cr.setSource(chain.get(rsnAndCol));
            }
        }
        return p;
    }

}
