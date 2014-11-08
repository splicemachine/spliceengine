package com.splicemachine.derby.impl.ast;


import com.google.common.base.*;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.derby.impl.sql.compile.HashNestedLoopJoinStrategy;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.AccessPath;
import org.apache.derby.iapi.sql.compile.Optimizable;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
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
            return pullUpPreds(j, ap);
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

    private JoinNode pullUpPreds(JoinNode j, AccessPath ap) throws StandardException {
        /*
         * We must allow duplicate entries here, because it's possible to have
         * multiple join conditions, and the equality check for Predicates
         * is very loose--it doesn't compare specific columns, only whether or
         * not they are both the same comparison operator. As a result,
         * removing duplicates here would remove more join conditions
         * than we can allow.
         *
         * We can't remove outright duplicates, but we can and SHOULD remove duplicates
         * which are the same exact object--thus, the use of an identity hash map here.
         */
        Collection<Predicate> toPullUp = Collections.newSetFromMap(new IdentityHashMap<Predicate, Boolean>());
        boolean removeFromBaseTable = !(ap.getJoinStrategy() instanceof HashNestedLoopJoinStrategy);
        ResultSetNode rightResultSet = j.getRightResultSet();
        pullPredicates(rightResultSet,toPullUp,evalableAtNode(j),removeFromBaseTable,rightResultSet,false);

        for (Predicate p: toPullUp){
            p = updatePredColRefsToNode(p, j);
            /*
             * We have pulled the predicate off of any relevant tables, so we have to ensure
             * that they aren't used as start keys anymore (which causes explosions when we try and generate stuff).
             */
            if(p.isStartKey())
                p.unmarkStartKey();
            if(p.isStopKey())
                p.unmarkStopKey();
            if(p.isQualifier())
                p.unmarkQualifier();
            j.addOptPredicate(p);
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Added pred %s to Join=%s.",
                        PredicateUtils.predToString.apply(p), j.getResultSetNumber()));
            }
        }

        return j;
    }

    private void pullPredicates(ResultSetNode next,Collection<Predicate> toPull,
                                com.google.common.base.Predicate<Predicate> joinScoped,
                                boolean removeFromBaseTable,
                                ResultSetNode top,boolean checkUsability) throws StandardException {
        /*
         * This method recursively pulls join predicates from the ResultSetNode specified (next); these
         * predicates are then pushed on to the JoinNode directly later.
         *
         * There are several strange aspects of this method, that are in place to avoid
         * awkwardness with the Derby query Optimizer.
         *
         * 1. When the "checkUsability" flag is set, we will first attempt to verify that
         *
         */
        if(next==null) return; //we are done
        com.google.common.base.Predicate<Predicate> pullCheck = Predicates.and(Predicates.not(evalableAtNode(next)),joinScoped);
        List<? extends Predicate> pulledPredicates = null;
        if(next instanceof ProjectRestrictNode){
            pulledPredicates = pullPredsFromPR((ProjectRestrictNode) next, pullCheck);
        }else if(next instanceof FromBaseTable){
            /*
             * If we are a HashNestedLoopJoin, then we can keep join predicates on the base node--in
             * fact, we need them there for correct performance. However, we ALSO need them
             * to be present on the Join node ( to ensure that the hash indices are properly found). This
             * is a pretty ugly attempt to ensure that this works correctly.
             */
            pulledPredicates= pullPredsFromTable((FromBaseTable) next, pullCheck, removeFromBaseTable);
        }else if(next instanceof IndexToBaseRowNode){
            /* Only pull from index if we are a HashNestedLoopJoin */
            pulledPredicates = pullPredsFromIndex((IndexToBaseRowNode) next, pullCheck);
        }

        if(pulledPredicates!=null && pulledPredicates.size()>0){
            if(checkUsability){
                for(Predicate predicate:pulledPredicates){
                /*
                 * Discard any join criteria which reference columns that don't exist in the top
                 * of the operation.
                 *
                 * This is to avoid a situation where you have a Union over two tables on the right,
                 * and you need to pull up join criteria. Unions only have one column type, but joins will
                 * have two (table 1 and table 2's join criteria). In this case, we need to discard the join
                 * predicate that refers to table 2, because it's redundant, and also because it will break
                 * when later trying to identify the hash join columns.
                 */
                    if(canBeUsed(predicate,top)){
                        toPull.add(predicate);
                    }
                }
            }else{
                toPull.addAll(pulledPredicates);
            }
        }

        if(next instanceof SingleChildResultSetNode){
            pullPredicates(((SingleChildResultSetNode)next).getChildResult(),toPull,pullCheck,removeFromBaseTable,top,checkUsability);
        }else if(next instanceof IndexToBaseRowNode){
            pullPredicates(((IndexToBaseRowNode)next).getSource(),toPull,pullCheck,removeFromBaseTable,top,checkUsability);
        }
        else if(next instanceof TableOperatorNode){
            TableOperatorNode ton = (TableOperatorNode)next;
            pullPredicates(ton.getLeftResultSet(),toPull,pullCheck,removeFromBaseTable,top,true);
            pullPredicates(ton.getRightResultSet(),toPull,pullCheck,removeFromBaseTable,top,true);
        }
    }

    private boolean canBeUsed(Predicate predicate, ResultSetNode target) {
        assert predicate.isJoinPredicate(): "Attempted to evaluate a non-join predicate";
        ResultColumnList targetCols = target.getResultColumns();
        AndNode andNode = predicate.getAndNode();
        ValueNode vn = andNode.getLeftOperand();
        if(vn instanceof BinaryRelationalOperatorNode){
            BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode)vn;
            ColumnReference colRef = (ColumnReference)bron.getLeftOperand();
            if(!findResultColumn(targetCols,colRef)){
           /*
            * We didn't find a result column which matched the left side of the
            * join predicate, so try again with the right
            */
                colRef = (ColumnReference)bron.getRightOperand();
                return findResultColumn(targetCols,colRef);
            }
        }
        //TODO -sf- deal with InListOperatorNode etc.
        return true;
    }

    private boolean findResultColumn(ResultColumnList targetCols, ColumnReference colRef) {
        for(ResultColumn rc:targetCols){
            if (matchesColumn(colRef, rc)) return true;
        }
        return false;
    }

    private boolean matchesColumn(ColumnReference colRef, ResultColumn rc) {
        ValueNode rcExpr = rc.getExpression();
        if(rcExpr instanceof ColumnReference){
            ColumnReference cRef = (ColumnReference)rcExpr;
            //these are *NOT* guaranteed to be non-null in all cases
            ColumnDescriptor resultDesc = cRef.getSource().getTableColumnDescriptor();
            ColumnDescriptor ourDesc = colRef.getSource().getTableColumnDescriptor();
            if(resultDesc!=null)
                return resultDesc.equals(ourDesc);
            else
                return true; //TODO -sf- this is probably incorrect
        }else if(rcExpr instanceof VirtualColumnNode){
            VirtualColumnNode vcn = (VirtualColumnNode)rcExpr;
            return matchesColumn(colRef, vcn.getSourceColumn());
        }
        return false;
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
                            PredicateUtils.predToString.apply(p), t.getResultSetNumber()));
                }
            }
            if(!pull || !shouldRemove)
                t.pushOptPredicate(p);
        }
        PredicateList storePl = t.storeRestrictionList;
        for(int i=0,s = storePl.size();i<s;i++){
            Predicate p = (Predicate)storePl.getOptPredicate(i);
            boolean pull = shouldPull.apply(p);
            if(pull && shouldRemove){
                storePl.removeOptPredicate(i);
//                pulled.add(p);
//                p.setPulled(true);
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Removed pred %s from Table=%s",
                            PredicateUtils.predToString.apply(p), t.getResultSetNumber()));
                }

            }
        }
        Collections.sort(pulled, new Comparator<Predicate>() { // Sort for Hash Join Key Ordering on Execution Side
			@Override
			public int compare(Predicate o1, Predicate o2) {
				return o1.getIndexPosition() - o2.getIndexPosition();
			}});               
        return pulled;
    }

    // Machinery for rewriting predicate column references (for NLJs)

    public JoinNode rewriteNLJColumnRefs(JoinNode j) throws StandardException {
    	if (LOG.isDebugEnabled())
    		SpliceLogUtils.debug(LOG, "rewriteNLJColumnRefs joinNode=%s", j);
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
