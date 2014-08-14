package com.splicemachine.derby.impl.ast;


import com.google.common.base.*;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.impl.sql.compile.*;
import org.apache.derby.impl.sql.compile.Predicate;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.util.*;


/**
 * This visitor modifies join predicates to avoid promiscuous column references in the plan.
 *
 * For hash-based join strategies, the visitor moves join predicates from joined tables
 * up to the join nodes themselves. Derby places join predicates on
 * FromBaseTable (or ProjectRestrict) nodes on the right-hand side (RHS) of the join, assuming
 * that a table on the RHS of the join will have access to the "current row" of a table
 * on the LHS at query execution time, an assumption which we know to be incorrect
 * for our joins. This visitor pulls the predicates up from the RHS to the join node itself.
 *
 * For NestedLoop joins, we leave the join predicate where it is (on the RHS) but make
 * sure that column references to the LHS are to the left-hand child of the join, not
 * to any further descendants (which may be across sink boundaries).
 *
 * User: pjt
 * Date: 7/8/13
 */

public class JoinConditionVisitor extends AbstractSpliceVisitor {

    private static Logger LOG = Logger.getLogger(JoinConditionVisitor.class);

    @Override
    public JoinNode visit(JoinNode j) throws StandardException {
    	SpliceLogUtils.debug(LOG, "visit joinNode=%s",j);
        AccessPath ap = ((Optimizable) j.getRightResultSet()).getTrulyTheBestAccessPath();
        if (RSUtils.isHashableJoin(ap)){
            if(ap.getJoinStrategy() instanceof HashNestedLoopJoinStrategy)
                return pullUpPreds(j,false);
            else
                return pullUpPreds(j,true);
        } else if (RSUtils.isNLJ(ap)){
            return rewriteNLJColumnRefs(j);
        } else {
            return j;
        }
    }

    @Override
    public JoinNode visit(HalfOuterJoinNode j) throws StandardException {
        return visit((JoinNode)j);
    }

    // Machinery for pulling up predicates (for hash-based joins)

    public JoinNode pullUpPreds(JoinNode j, boolean ignoreIndex) throws StandardException {
        List<Predicate> toPullUp = new LinkedList<Predicate>();

        // Collect PRs, FBTs until a binary node (Union, Join) found, or end
        Iterable<ResultSetNode> rightsUntilBinary = Iterables.filter(
                RSUtils.nodesUntilBinaryNode(j.getRightResultSet()),
                RSUtils.rsnHasPreds);

        com.google.common.base.Predicate<Predicate> joinScoped = evalableAtNode(j);

        for (ResultSetNode rsn: rightsUntilBinary) {
            // Encode whether to pull up predicate to join:
            //  when can't evaluate on node but can evaluate at join
            com.google.common.base.Predicate<Predicate> shouldPull =
                    Predicates.and(Predicates.not(evalableAtNode(rsn)), joinScoped);
            if(rsn instanceof ProjectRestrictNode)
                toPullUp.addAll(pullPredsFromPR((ProjectRestrictNode)rsn,shouldPull));
            else if(rsn instanceof FromBaseTable){
                /*
                 * If we are a HashNestedLoopJoin, then we can keep join predicates on the base node--in
                 * fact, we need them there for correct performance. However, we ALSO need them
                 * to be present on the Join node ( to ensure that the hash indices are properly found). This
                 * is a pretty ugly attempt to ensure that this works correctly.
                 */
                toPullUp.addAll(pullPredsFromTable((FromBaseTable)rsn,shouldPull,ignoreIndex)); //avoid pulling predicates from base table when doing a hash nlj
            }else if(rsn instanceof IndexToBaseRowNode){
                if(!ignoreIndex){
                    List<? extends Predicate> c = pullPredsFromIndex((IndexToBaseRowNode) rsn, shouldPull);
                    toPullUp.addAll(c);
                }
            }else
                throw new IllegalArgumentException("Programmer error: unable to find proper class for pulling predicates: "+ rsn);
        }

        for (Predicate p: toPullUp){
            p = updatePredColRefsToNode(p, j);
            j.addOptPredicate(p);
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Added pred %s to Join=%s.",
                        PredicateUtils.predToString.apply(p), j.getResultSetNumber()));
            }
        }

        return j;
    }

    private List<? extends Predicate> pullPredsFromIndex(IndexToBaseRowNode rsn,
                                                         com.google.common.base.Predicate<Predicate> shouldPull) throws StandardException {
        List<Predicate> pulled = new LinkedList<Predicate>();
        if (rsn.restrictionList != null) {
            for (int i = rsn.restrictionList.size() - 1; i >= 0; i--) {
                Predicate p = (Predicate)rsn.restrictionList.getOptPredicate(i);
                if (shouldPull.apply(p)) {
                    pulled.add(p);
                    if (LOG.isDebugEnabled()){
                        LOG.debug(String.format("Pulled pred %s from PR=%s",
                                PredicateUtils.predToString.apply(p),
                                rsn.getResultSetNumber()));
                    }
                    rsn.restrictionList.removeOptPredicate(i);
                }
            }
        }
        return pulled;
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
                    if (LOG.isDebugEnabled()){
                        LOG.debug(String.format("Pulled pred %s from PR=%s",
                                                       PredicateUtils.predToString.apply(p),
                                                       pr.getResultSetNumber()));
                    }
                    pr.restrictionList.removeOptPredicate(i);
                }
            }
        }
        return pulled;
    }

    public List<Predicate> pullPredsFromTable(FromBaseTable t,
                                              com.google.common.base.Predicate<Predicate> shouldPull,
                                              boolean shouldRemove)
            throws StandardException {
        List<Predicate> pulled = new LinkedList<Predicate>();
        PredicateList pl = new PredicateList();
        t.pullOptPredicates(pl);
        for (int i = 0, s = pl.size(); i < s; i++) {
            Predicate p = (Predicate)pl.getOptPredicate(i);
            boolean pull = shouldPull.apply(p);
            if (pull) {
                pulled.add(p);
                p.setPulled(true);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Pulled pred %s from Table=%s",
                            PredicateUtils.predToString.apply((Predicate) p), t.getResultSetNumber()));
                }
            }
            if(!pull || !shouldRemove)
                t.pushOptPredicate(p);
        }
        return pulled;
    }

    // Machinery for rewriting predicate column references (for NLJs)

    public JoinNode rewriteNLJColumnRefs(JoinNode j) throws StandardException {
    	if (LOG.isDebugEnabled())
    		SpliceLogUtils.debug(LOG, "rewriteNLJColumnRefs joinNode=%s" + j);
        List<Predicate> joinPreds = new LinkedList<Predicate>();

        // Collect PRs, FBTs until a binary node (Union, Join) found, or end
        Iterable<ResultSetNode> rightsUntilBinary = Iterables.filter(
                RSUtils.nodesUntilBinaryNode(j.getRightResultSet()),
                RSUtils.rsnHasPreds);
        
        com.google.common.base.Predicate<Predicate> joinScoped = evalableAtNode(j);

    	if (LOG.isDebugEnabled())
    		SpliceLogUtils.debug(LOG, "joinScoped joinScoped=%s",joinScoped);
        
        for (ResultSetNode rsn: rightsUntilBinary) {
        	if (LOG.isDebugEnabled())
        		SpliceLogUtils.debug(LOG, "rewriteNLJColumnRefs rightsUntilBinary=%s",rsn);
        	// Encode whether to pull up predicate to join:
            //  when can't evaluate on node but can evaluate at join
            com.google.common.base.Predicate<Predicate> predOfInterest =
                    Predicates.and(Predicates.not(evalableAtNode(rsn)), joinScoped);
            joinPreds.addAll(Collections2
                                 .filter(RSUtils.collectExpressionNodes(rsn, Predicate.class),
                                            predOfInterest));
        }

        for (Predicate p: joinPreds) {
        	updatePredColRefsToNode(p, j.getLeftResultSet());
        }
        return j;
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
     * Rewrites column references in a Predicate to point to ResultColumns from the passed node.
     */
    public Predicate updatePredColRefsToNode(Predicate p, ResultSetNode n)
            throws StandardException
    {
    	
    	if (LOG.isDebugEnabled())
    		SpliceLogUtils.debug(LOG, "updatePredColRefsToNode predicate=%s, resultSetNode=%s",p,n);
    	
        ResultColumnList rcl = n.getResultColumns();
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
