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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.ast;


import org.spark_project.guava.base.Function;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.AccessPath;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.impl.sql.compile.ColumnReference;
import com.splicemachine.db.impl.sql.compile.FromBaseTable;
import com.splicemachine.db.impl.sql.compile.HalfOuterJoinNode;
import com.splicemachine.db.impl.sql.compile.IndexToBaseRowNode;
import com.splicemachine.db.impl.sql.compile.JoinNode;
import com.splicemachine.db.impl.sql.compile.Predicate;
import com.splicemachine.db.impl.sql.compile.PredicateList;
import com.splicemachine.db.impl.sql.compile.ProjectRestrictNode;
import com.splicemachine.db.impl.sql.compile.ResultColumn;
import com.splicemachine.db.impl.sql.compile.ResultColumnList;
import com.splicemachine.db.impl.sql.compile.ResultSetNode;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Predicates;
import org.spark_project.guava.collect.Collections2;
import org.spark_project.guava.collect.Iterables;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Sets;
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
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("visit joinNode=%s",j));
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
        List<Predicate> toPullUp = new LinkedList<Predicate>();

        // Collect PRs, FBTs until a binary node (Union, Join) found, or end
        Iterable<ResultSetNode> rightsUntilBinary = Iterables.filter(
                RSUtils.nodesUntilBinaryNode(j.getRightResultSet()),
                RSUtils.rsnHasPreds);

        org.spark_project.guava.base.Predicate<Predicate> joinScoped = evalableAtNode(j);

        for (ResultSetNode rsn: rightsUntilBinary) {
            List<? extends Predicate> c = null;
            // Encode whether to pull up predicate to join:
            //  when can't evaluate on node but can evaluate at join
            org.spark_project.guava.base.Predicate<Predicate> shouldPull =
                    Predicates.and(Predicates.not(evalableAtNode(rsn)), joinScoped);
            if(rsn instanceof ProjectRestrictNode)
                c = pullPredsFromPR((ProjectRestrictNode)rsn,shouldPull);
            else if(rsn instanceof FromBaseTable){
                /*
                 * If we are a HashNestedLoopJoin, then we can keep join predicates on the base node--in
                 * fact, we need them there for correct performance. However, we ALSO need them
                 * to be present on the Join node ( to ensure that the hash indices are properly found). This
                 * is a pretty ugly attempt to ensure that this works correctly.
                 */
                // Removing HashNestedLoopJoinStrategy: Implementation Detail not a join strategy
//                boolean removeFromBaseTable = !(ap.getJoinStrategy() instanceof HashNestedLoopJoinStrategy);
                c = pullPredsFromTable((FromBaseTable)rsn,shouldPull,true);
            }else if(rsn instanceof IndexToBaseRowNode){
                /* Only pull from index if we are a HashNestedLoopJoin */
                boolean pullFromIndex = true;//(ap.getJoinStrategy() instanceof HashNestedLoopJoinStrategy);
                if(pullFromIndex){
                    c = pullPredsFromIndex((IndexToBaseRowNode) rsn, shouldPull);
                }
            }else
                throw new IllegalArgumentException("Programmer error: unable to find proper class for pulling predicates: "+ rsn);

            for (Predicate p:c) {
                if (!toPullUp.contains(p))
                    toPullUp.addAll(c);
            }
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
                                                                                     org.spark_project.guava.base.Predicate<Predicate> shouldPull) throws StandardException {
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
                                           org.spark_project.guava.base.Predicate<Predicate> shouldPull)
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
                                              org.spark_project.guava.base.Predicate<Predicate> shouldPull,
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
    		LOG.debug(String.format("rewriteNLJColumnRefs joinNode=%s", j));
        List<Predicate> joinPreds = new LinkedList<Predicate>();

        // Collect PRs, FBTs until a binary node (Union, Join) found, or end
        Iterable<ResultSetNode> rightsUntilBinary = Iterables.filter(
                RSUtils.nodesUntilBinaryNode(j.getRightResultSet()),
                RSUtils.rsnHasPreds);

        org.spark_project.guava.base.Predicate<Predicate> joinScoped = evalableAtNode(j);

    	if (LOG.isDebugEnabled())
    		LOG.debug(String.format("joinScoped joinScoped=%s",joinScoped));
        
        for (ResultSetNode rsn: rightsUntilBinary) {
        	if (LOG.isDebugEnabled())
        		LOG.debug(String.format("rewriteNLJColumnRefs rightsUntilBinary=%s",rsn));
        	// Encode whether to pull up predicate to join:
            //  when can't evaluate on node but can evaluate at join
            org.spark_project.guava.base.Predicate<Predicate> predOfInterest =
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
                                return ColumnUtils.RSCoordinate(cr.getSource()).getLeft();
                            }
                        }));
    }


    /**
     * Returns a fn that returns true if a Predicate can be evaluated at the node rsn
     */
    public static org.spark_project.guava.base.Predicate<Predicate> evalableAtNode(final ResultSetNode rsn)
            throws StandardException {
        final Set<Integer> rsns = Sets.newHashSet(Lists.transform(RSUtils.getSelfAndDescendants(rsn), RSUtils.rsNum));
        return new org.spark_project.guava.base.Predicate<Predicate>() {
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
            throws StandardException {
    	if (LOG.isDebugEnabled())
    		LOG.debug(String.format("updatePredColRefsToNode predicate=%s, resultSetNode=%s",p,n));
    	
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
