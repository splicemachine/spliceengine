package com.splicemachine.derby.impl.ast;


import com.google.common.base.*;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.OptimizablePredicate;
import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.impl.sql.compile.*;
import org.apache.derby.impl.sql.compile.Predicate;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.util.*;


/**
 * This visitor moves join predicates from joined tables to the join nodes themselves for
 * MergeSort joins. During optimization, Derby pushes down the predicates to FromBaseTable
 * nodes, assuming that a table on one side of the join will have access to the "current
 * row" of a table on the other side at query execution time, an assumption which we know
 * to be incorrect for MSJ. This visitor reverses
 *
 * User: pjt
 * Date: 7/8/13
 */

public class MSJJoinConditionVisitor extends AbstractSpliceVisitor {

    private static Logger LOG = Logger.getLogger(MSJJoinConditionVisitor.class);

    public static boolean predicateIsEvalable(OptimizablePredicate p, ResultSetNode n) {
        JBitSet pRefs = (JBitSet) p.getReferencedMap().clone();
        JBitSet nRefs = n.getReferencedTableMap();
        // make pRefs represent union of tables referenced
        pRefs.or(nRefs);
        // make pRefs represent union of tables referenced minus tables referenced by n
        pRefs.xor(nRefs);
        // if all bits in pRefs are zero, then n refers to all tables referred to by p,
        // & p is therefore evalable in context of n
        return pRefs.getFirstSetBit() == -1;
    }


    private List<OptimizablePredicate> pulledPreds = new ArrayList<OptimizablePredicate>();

    @Override
    public FromBaseTable visit(FromBaseTable t) throws StandardException {
        return pullPredsFromTable(t);
    }

    @Override
    public ProjectRestrictNode visit(ProjectRestrictNode pr) throws StandardException {
        return pullPredsFromPR(pr);
    }

    @Override
    public JoinNode visit(JoinNode j) throws StandardException {
        return addPredsToJoin(j);
    }

    @Override
    public JoinNode visit(HalfOuterJoinNode j) throws StandardException {
        return addPredsToJoin(j);
    }

    public ProjectRestrictNode pullPredsFromPR(ProjectRestrictNode pr) throws StandardException {
        if (RSUtils.isHashableJoin(pr.getTrulyTheBestAccessPath()) &&
                pr.restrictionList != null) {
            for (int i = pr.restrictionList.size() - 1; i >= 0; i--) {
                OptimizablePredicate p = pr.restrictionList.getOptPredicate(i);
                if (((Predicate)p).isJoinPredicate() && !predicateIsEvalable(p, pr)) {
                    pulledPreds.add(p);
                    LOG.debug(String.format("Pulled pred %s from PR=%s",
                            PredicateUtils.predToString.apply((Predicate)p), pr.getResultSetNumber()));
                    pr.restrictionList.removeOptPredicate(i);
                }
            }
        }
        return pr;
    }

    public FromBaseTable pullPredsFromTable(FromBaseTable t) throws StandardException {
        if (RSUtils.isHashableJoin(t.getTrulyTheBestAccessPath())) {
            PredicateList pl = new PredicateList();
            t.pullOptPredicates(pl);
            for (int i = 0, s = pl.size(); i < s; i++) {
                OptimizablePredicate p = pl.getOptPredicate(i);
                if (((Predicate)p).isJoinPredicate() && !predicateIsEvalable(p, t)) {
                    pulledPreds.add(p);
                    LOG.debug(String.format("Pulled pred %s from Table=%s",
                            PredicateUtils.predToString.apply((Predicate) p), t.getResultSetNumber()));
                } else {
                    t.pushOptPredicate(p);
                }
            }
        }
        return t;
    }

    public JoinNode addPredsToJoin(JoinNode j) throws StandardException {
        int pSize = pulledPreds.size();
        OptimizablePredicate[] currentPreds = pulledPreds.toArray(new OptimizablePredicate[pSize]);

        for (OptimizablePredicate p : currentPreds) {
            if (predicateIsEvalable(p, j)) {
                p = updatePredColRefsToJoin((Predicate) p, j);
                j.addOptPredicate(p);
                pulledPreds.remove(p);
                LOG.debug(String.format("Added pred %s to Join=%s.\nRemaining pulled preds: %s",
                        PredicateUtils.predToString.apply((Predicate)p), j.getResultSetNumber(), pulledPreds));
            }
        }
        return j;
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
