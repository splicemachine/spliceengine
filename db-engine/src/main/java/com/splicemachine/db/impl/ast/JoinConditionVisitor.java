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


import com.carrotsearch.hppc.LongLongHashMap;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.compile.*;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.impl.sql.compile.*;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;
import org.spark_project.guava.base.Function;
import org.spark_project.guava.base.Predicates;
import org.spark_project.guava.collect.Collections2;
import org.spark_project.guava.collect.Iterables;
import org.spark_project.guava.collect.Lists;
import org.spark_project.guava.collect.Sets;
import org.spark_project.guava.primitives.Ints;

import java.sql.SQLException;
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
    private LongLongHashMap joinChainMap;

    private void initializeMap(QueryTreeNode v)  throws StandardException {
        if (joinChainMap == null)
            joinChainMap = v.childParentMap();
    }

    @Override
    public Visitable visit(ProjectRestrictNode node) throws StandardException {
        if (node.hasSubqueries())
            initializeMap(node);
        return super.visit(node);
    }

    @Override
    public UnionNode visit(UnionNode node) throws StandardException {
        initializeMap(node);
        return node;
    }

    @Override
    public IntersectOrExceptNode visit(IntersectOrExceptNode node) throws StandardException {
        initializeMap(node);
        return node;
    }

    @Override
    public JoinNode visit(JoinNode j) throws StandardException {
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("visit joinNode=%s",j));
        initializeMap(j);
        AccessPath ap = ((Optimizable) j.getRightResultSet()).getTrulyTheBestAccessPath();
        if (RSUtils.isHashableJoin(ap) || RSUtils.isCrossJoin(ap)){
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

    @Override
    public JoinNode visit(FullOuterJoinNode j) throws StandardException {
        return visit((JoinNode)j);
    }

    // Machinery for pulling up predicates (for hash-based joins)

    private JoinNode pullUpPreds(JoinNode j, AccessPath ap) throws StandardException {
        List<Predicate> toPullUp = new LinkedList<>();

        // Collect PRs, FBTs until a binary node (Union, Join) found, or end
        Iterable<ResultSetNode> rightsUntilBinary = Iterables.filter(
                RSUtils.nodesUntilBinaryNode(j.getRightResultSet()),
                RSUtils.rsnHasPreds);

        org.spark_project.guava.base.Predicate<Predicate> joinScoped = evalableAtNode(j);
        org.spark_project.guava.base.Predicate<Predicate> isFullJoinPredicate = pred -> pred.isFullJoinPredicate();

        ResultSetNode parent = null;
        for (ResultSetNode rsn: rightsUntilBinary) {
            List<? extends Predicate> c = null;
            // Encode whether to pull up predicate to join:
            //  when can't evaluate on node but can evaluate at join
            org.spark_project.guava.base.Predicate<Predicate> shouldPull =
                    Predicates.and(Predicates.or(Predicates.not(evalableAtNode(rsn)), isFullJoinPredicate), joinScoped);
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
                /* predicate on the non-covering index node must be store predicate, there is no need to pull up.
                   so skip the predicate pullup logic for this scenario
                 */
                if (parent != null && parent instanceof IndexToBaseRowNode)
                    c = Collections.emptyList();
                else
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
            parent = rsn;
        }
       // joinChainMap = j.rsnChainMapV2();
        for (Predicate p: toPullUp){
            p = updatePredColRefsToNode(p, j);
            j.addOptPredicate(p);
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Added pred %s to Join=%s.",
                        PredicateUtils.predToString.apply(p), j.getResultSetNumber()));
            }
        }

        // the joinPrediates are sorted in the order of the predicate's indexPosition which is important to merge join,
        // so we want to maintain the order
        List<Predicate> equiJoinPreds = PredicateUtils.PLtoList(j.joinPredicates);
        Iterator<Predicate> it = equiJoinPreds.iterator();
        boolean isMergeJoin = j.isMergeJoin();
        while (it.hasNext()) {
            Predicate p = it.next();
            if (!PredicateUtils.isEquiJoinPred.apply(p))
                it.remove();
            else if (isMergeJoin) {
                if (p.getIndexPosition() < 0) {

                    // the predicate is not on an index column, so no sort order info availabe,
                    // this column/predicate cannot be used by merge join to compare row and determine when
                    // to stop searching the matching right row for a left row, so take it out from the equiJoinPreds.
                    // this is consistent with MergeJoinStrategy.mergeable() which also does not use such predicate
                    // to qualify merge join
                    it.remove();
                } else {
                    // when we get here, the join predicate much be an equality predicate on an index column
                    // but we still need to disqualify predicate with expression as MergeJoinStrategy.mergeable() does
                    // not consider such predicates. Including such predicates could lead to wrong result, for example:
                    // given "a2=-a1", a1 and a2 will be negatively correlated.
                    ValueNode valueNode = p.getAndNode().getLeftOperand();
                    if (valueNode instanceof BinaryRelationalOperatorNode) {
                        if ((((BinaryRelationalOperatorNode) valueNode).getLeftOperand() instanceof ColumnReference) &&
                                ((BinaryRelationalOperatorNode) valueNode).getRightOperand() instanceof ColumnReference)
                            continue;
                    }
                    it.remove();
                }
            }
        }

        // populate rightHashKeySortOrders for merge join, we need this information when comparing the source rows for
        // a merge join
        if (isMergeJoin) {
            // get the index's ascDesc info
            ConglomerateDescriptor currentCd=((FromTable)j.getRightResultSet()).getTrulyTheBestAccessPath().getConglomerateDescriptor();
            IndexRowGenerator innerRowGen= currentCd != null ? currentCd.getIndexDescriptor(): null;

            boolean[] ascInfo = (innerRowGen != null)? innerRowGen.isAscending(): null;

            j.rightHashKeySortOrders = new int[equiJoinPreds.size()];
            int index = 0;

            for (Predicate p: equiJoinPreds) {
                int indexPosition = p.getIndexPosition();
                // the predicate is on index column, record the sort order of this column
                // 1 means asc, 0 means desc
                if (ascInfo != null) {
                    j.rightHashKeySortOrders[index++] = ascInfo[indexPosition] ? 1 : 0;
                } else {
                    // primary key does not have explicit ascInfo set, but it is always in asc order
                    j.rightHashKeySortOrders[index++] = 1;
                }
            }
        }

        Pair<List<Integer>, List<Integer>> indices = findHashIndices(j, equiJoinPreds);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Hash key indices found for Join n=%s: %s", j.getResultSetNumber(), indices));
        j.leftHashKeys = Ints.toArray(indices.getLeft());
        j.rightHashKeys = Ints.toArray(indices.getRight());

        return j;
    }

    private List<? extends Predicate> pullPredsFromIndex(IndexToBaseRowNode rsn,
                                                                                     org.spark_project.guava.base.Predicate<Predicate> shouldPull) throws StandardException {
        List<Predicate> pulled = new LinkedList<>();
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
        List<Predicate> pulled = new LinkedList<>();
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
        List<Predicate> pulled = new LinkedList<>();
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
        List<Predicate> joinPreds = new LinkedList<>();

        // Collect PRs, FBTs from the right source of NLJ. We need to collect ResultSet until leave nodes
        // except for Except and Intersect node.
        // because for NLJ, join condition could be pushed down under union operation and joins.
        Iterable<ResultSetNode> rights = Iterables.filter(
                RSUtils.nodesUntilBinaryNodeExcludeUnion(j.getRightResultSet()),
                RSUtils.rsnHasPreds);

        org.spark_project.guava.base.Predicate<Predicate> joinScoped = evalableAtNode(j);

    	if (LOG.isDebugEnabled())
    		LOG.debug(String.format("joinScoped joinScoped=%s",joinScoped));

        for (ResultSetNode rsn: rights) {
        	if (LOG.isDebugEnabled())
        		LOG.debug(String.format("rewriteNLJColumnRefs rights=%s",rsn));
        	// Encode whether to pull up predicate to join:
            //  when can't evaluate on node but can evaluate at join
            org.spark_project.guava.base.Predicate<Predicate> predOfInterest =
                    Predicates.and(Predicates.not(evalableAtNode(rsn)), joinScoped);
            joinPreds.addAll(Collections2
                     .filter(RSUtils.collectExpressionNodes(rsn, Predicate.class),
                             predOfInterest));
        }

      //  joinChainMap = j.rsnChainMapV2();

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
                Iterables.filter(Lists.transform(RSUtils.collectNodes(p, ColumnReference.class),
                        new Function<ColumnReference, Integer>() {
                            @Override
                            public Integer apply(ColumnReference cr) {
                                // ColumnReference pointing to a subquery may have source set to null
                                return (int) ((cr.getSource()==null)?-1:cr.getSource().getCoordinates() >> 32);
                            }
                        }), (rsnNumber -> rsnNumber >= 0)));
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
        List<ColumnReference> predCRs = RSUtils.collectNodes(p, ColumnReference.class);
        for (ColumnReference cr: predCRs){
            boolean ignore = false;
            ResultColumn rc = cr.getSource();
            long rsnAndCol = rc.getCoordinates();
            if (!joinChainMap.containsKey(rsnAndCol)) {
                continue; // huh?
            }
            while (  (int)(rsnAndCol>>32) != n.getResultSetNumber()) {
                if (!joinChainMap.containsKey(rsnAndCol)) {
                    ignore = true;
                    break;
                }
                rsnAndCol = joinChainMap.get(rsnAndCol);
            }
            if (!ignore)
                cr.setSource(n.getResultColumns().elementAt( ((int) rsnAndCol)-1 ));
        }
        return p;
    }
    public Integer translateToIndexOnList(ColumnReference cr,
                                                 ResultColumnList rcl,
                                                 ValueNode operand,
                                                 ResultSetNode resultSetNode,
                                                 BinaryRelationalOperatorNode brop,
                                                 JoinNode  joinNode)
            throws StandardException{
        boolean hasSubqueryNode = false;
        if (cr.getSource().getExpression() instanceof VirtualColumnNode) {
            VirtualColumnNode n = (VirtualColumnNode) cr.getSource().getExpression();
            if (n.getSourceResultSet() instanceof FromSubquery) {
                hasSubqueryNode = true;
            }
        }

        if (operand instanceof ColumnReference) {
            long colCoord = (cr.getSourceResultColumn() != null && !hasSubqueryNode ? cr.getSourceResultColumn() : cr.getOrigSourceResultColumn()).getCoordinates();
            while (  (int)(colCoord>>32) != resultSetNode.getResultSetNumber()) {
                if (!joinChainMap.containsKey(colCoord)) {
                    throw new RuntimeException(String.format("Unable to find ColRef %s in RCL %s", cr, rcl));
                }
                colCoord = joinChainMap.get(colCoord);
            }
            return rcl.elementAt( ((int) colCoord)-1).getVirtualColumnId() - 1; // translate from 1-based to 0-based index
        }
        else {
            // Add a result column to the child result set
            ResultColumn resultColumn = addResultColumnToChild(cr, operand, resultSetNode, rcl);

            //Change operand's column reference to the column that has just added to child result set
            resultColumn = resetColumnReference(resultColumn,operand, brop, joinNode, resultSetNode);

            // Add a column to the result column list of the join node
            rebuildJoinNodeRCL(joinNode, resultSetNode, resultColumn);

            // Hash column position is the end of result column list of child result set
            return rcl.size()-1;
        }
    }

    private static ResultColumn resetColumnReference(ResultColumn resultColumn,
                                                     ValueNode operand,
                                                     BinaryRelationalOperatorNode brop,
                                                     JoinNode joinNode,
                                                     ResultSetNode resultSetNode) throws StandardException {

        try {
            LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
            NodeFactory nodeFactory = lcc.getLanguageConnectionFactory().
                    getNodeFactory();

            ColumnReference generatedRef = (ColumnReference) nodeFactory.getNode(
                    C_NodeTypes.COLUMN_REFERENCE,
                    resultColumn.getName(),
                    null,
                    ContextService.getService().getCurrentContextManager());
            VirtualColumnNode vnode = (VirtualColumnNode) nodeFactory.getNode(C_NodeTypes.VIRTUAL_COLUMN_NODE,
                    resultSetNode, // source result set.
                    resultColumn,
                    resultSetNode.getResultColumns().size(),
                    ContextService.getService().getCurrentContextManager());

            resultColumn = (ResultColumn) nodeFactory.getNode(C_NodeTypes.RESULT_COLUMN,
                    resultColumn.getName(),
                    vnode,
                    ContextService.getService().getCurrentContextManager());
            resultColumn.markGenerated();
            resultColumn.setResultSetNumber(joinNode.getResultSetNumber());
            generatedRef.setSource(resultColumn);
            if (brop.getLeftOperand() == operand) {
                List<ColumnReference> columnReferences = brop.getLeftOperand().getHashableJoinColumnReference();
                assert columnReferences != null && columnReferences.size() == 1: "one column reference is expected!";
                generatedRef.setTableNumber(columnReferences.get(0).getTableNumber());
                brop.setLeftOperand(generatedRef);
            }
            else {
                List<ColumnReference> columnReferences = brop.getRightOperand().getHashableJoinColumnReference();
                assert columnReferences != null && columnReferences.size() == 1: "one column reference is expected!";
                generatedRef.setTableNumber(columnReferences.get(0).getTableNumber());
                brop.setRightOperand(generatedRef);
            }
            return resultColumn;
        }
        catch (SQLException e) {
            throw StandardException.newException(e.getSQLState());
        }
    }

    private static ResultColumn addResultColumnToChild(ColumnReference cr,
                                                       ValueNode operand,
                                                       ResultSetNode resultSetNode,
                                                       ResultColumnList rcl) throws StandardException{
        try {
            LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
            NodeFactory nodeFactory = lcc.getLanguageConnectionFactory().
                    getNodeFactory();

            // Add a result column and return virtual column Id
            ResultColumn rc = cr.getSource();
            assert rc.getExpression() instanceof VirtualColumnNode;
            VirtualColumnNode vn = (VirtualColumnNode) rc.getExpression();
            rc = vn.getSourceColumn();

            // construct a result column and add to result column list of child result set
            ColumnReference generatedRef = (ColumnReference) nodeFactory.getNode(
                    C_NodeTypes.COLUMN_REFERENCE,
                    rc.getName(),
                    null,
                    ContextService.getService().getCurrentContextManager());

            assert rc.getExpression() instanceof VirtualColumnNode;
            vn = (VirtualColumnNode) rc.getExpression();
            generatedRef.setSource(vn.getSourceColumn());
            List<ColumnReference> columnReferences = operand.getHashableJoinColumnReference();
            assert columnReferences != null && columnReferences.size() == 1: "one column reference is expected!";
            generatedRef.setTableNumber(columnReferences.get(0).getTableNumber());
            operand.setHashableJoinColumnReference(generatedRef);

            ResultColumn resultColumn =
                    (ResultColumn) nodeFactory.getNode(C_NodeTypes.RESULT_COLUMN,
                            generatedRef.getColumnName(),
                            operand,
                            ContextService.getService().getCurrentContextManager());
            resultColumn.markGenerated();
            resultColumn.setResultSetNumber(resultSetNode.getResultSetNumber());
            resultColumn.setVirtualColumnId(rcl.size());
            rcl.addResultColumn(resultColumn);

            return resultColumn;
        }
        catch (SQLException e) {
            throw StandardException.newException(e.getSQLState());
        }
    }
    public Pair<List<Integer>, List<Integer>> findHashIndices(final JoinNode node, Collection<Predicate> equiJoinPreds)
            throws StandardException {
        List<Integer> leftIndices = Lists.newArrayListWithCapacity(equiJoinPreds.size());
        List<Integer> rightIndices = Lists.newArrayListWithCapacity(equiJoinPreds.size());
        org.spark_project.guava.base.Predicate<ResultColumn> isLeftRef = RSUtils.pointsTo(node.getLeftResultSet());
        ResultColumnList leftRCL = node.getLeftResultSet().getResultColumns();
        ResultColumnList rightRCL = node.getRightResultSet().getResultColumns();

        for (Predicate p : equiJoinPreds) {
            AndNode andNode = p.getAndNode();
            assert andNode.getLeftOperand() instanceof BinaryRelationalOperatorNode;
            BinaryRelationalOperatorNode brop = (BinaryRelationalOperatorNode)andNode.getLeftOperand();
            ValueNode leftOperand = brop.getLeftOperand();
            ValueNode rightOperand = brop.getRightOperand();
            List<ColumnReference> lcr = RSUtils.collectNodes(leftOperand, ColumnReference.class);
            List<ColumnReference> rcr = RSUtils.collectNodes(rightOperand, ColumnReference.class);

            for (ColumnReference cr : lcr) {
                if (isLeftRef.apply(cr.getSourceResultColumn()!=null?cr.getSourceResultColumn():cr.getOrigSourceResultColumn())) {
                    leftIndices.add(translateToIndexOnList(cr, leftRCL, leftOperand, node.getLeftResultSet(), brop, node));
                } else {
                    rightIndices.add(translateToIndexOnList(cr, rightRCL, leftOperand, node.getRightResultSet(), brop, node));
                }
            }

            for (ColumnReference cr : rcr) {
                if (isLeftRef.apply(cr.getSourceResultColumn()!=null?cr.getSourceResultColumn():cr.getOrigSourceResultColumn())) {
                    leftIndices.add(translateToIndexOnList(cr, leftRCL, rightOperand, node.getLeftResultSet(), brop, node));
                } else {
                    rightIndices.add(translateToIndexOnList(cr, rightRCL, rightOperand, node.getRightResultSet(), brop, node));
                }
            }
        }

        return Pair.of(leftIndices, rightIndices);
    }

    private static void rebuildJoinNodeRCL(JoinNode node,
                                           ResultSetNode child,
                                           ResultColumn rc) throws StandardException{

        boolean isLeft = node.getLeftResultSet() == child;
        ResultColumnList rcl = node.getResultColumns();
        boolean wasRightOuterJoin = false;

        if (node instanceof HalfOuterJoinNode)
        {
            wasRightOuterJoin = ((HalfOuterJoinNode)node).isRightOuterJoin();
        }

        if (isLeft && !wasRightOuterJoin || !isLeft && wasRightOuterJoin) {
            int size = rcl.size();
            // make a copy of result column list
            ResultColumnList temp = new ResultColumnList();
            for (int i = 0; i < size; ++i) {
                temp.addResultColumn(rcl.elementAt(0));
                rcl.removeElementAt(0);
            }

            // copy result columns that reference to left result set
            Iterator<ResultColumn> iter=temp.iterator();
            while(iter.hasNext()){
                ResultColumn resultColumn = iter.next();
                if (resultColumn.isFromLeftChild()) {
                    rcl.addResultColumn(resultColumn);
                    iter.remove();
                }else break;
            }

            // Add a new column to join result column list
            rcl.addResultColumn(rc);

            // copy columns that reference to the right result set
            while(!temp.isEmpty()) {
                rcl.addResultColumn(temp.elementAt(0));
                temp.removeElementAt(0);
            }
        }
        else {
            rc.setFromLeftChild(false);
            rcl.addResultColumn(rc);
        }
    }


    @Override
    public boolean isPostOrder() {
        // Default to PostOrder traversal, i.e. bottom-up
        return false;
    }
}
