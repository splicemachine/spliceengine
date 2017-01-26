/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations.batchonce;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.C_NodeTypes;
import com.splicemachine.db.iapi.sql.compile.Visitable;
import com.splicemachine.db.impl.ast.AbstractSpliceVisitor;
import com.splicemachine.db.impl.ast.RSUtils;
import com.splicemachine.db.impl.sql.compile.*;
import com.splicemachine.utils.Pair;
import org.spark_project.guava.collect.Lists;

import java.util.List;

/**
 * Replaces a ProjectRestrictNode under an Update with a BatchOnceNode.  See BatchOnceNode and BatchOnceOperation for
 * details.
 */
public class BatchOnceVisitor extends AbstractSpliceVisitor {

    @Override
    public boolean skipChildren(Visitable node) {
        return node instanceof UpdateNode;
    }

    @Override
    public Visitable visit(UpdateNode updateNode) throws StandardException {
        super.visit(updateNode);
        ResultSetNode resultSetNode = updateNode.getResultSetNode();

        //
        // expecting a ProjectRestrictNode directly under the update
        //
        if (!(resultSetNode instanceof ProjectRestrictNode)) {
            return updateNode;
        }

        ProjectRestrictNode prUnderUpdate = (ProjectRestrictNode) resultSetNode;

        //
        // The ProjectRestrictNode result columns must have exactly one expression subquery tree
        // with the required shape.
        //
        int subqueryCount = 0;
        SubqueryNode subqueryNode = null;
        ResultColumn subqueryResultColumn = null;
        List<Pair<ColumnReference, ColumnReference>> correlatedSubqueryColRefList = null;
        for (ResultColumn resultColumn : prUnderUpdate.getResultColumns()) {
            ValueNode resultColumnExpression = resultColumn.getExpression();
            if (resultColumnExpression instanceof SubqueryNode) {
                subqueryCount++;
                subqueryNode = (SubqueryNode) resultColumnExpression;
                correlatedSubqueryColRefList = isApplicableSubqueryNode(subqueryNode);
                if (correlatedSubqueryColRefList != null && correlatedSubqueryColRefList.size() > 0) {
                    subqueryResultColumn = resultColumn;
                }
            }
        }
        if (subqueryCount != 1 || subqueryResultColumn == null) {
            return updateNode;
        }

        //
        // Expecting the subquery Tree to look like this: SubqueryNode -> PR -> PR -> FromBaseTable
        //
        ResultSetNode pr1 = subqueryNode.getResultSet();
        if (!(pr1 instanceof ProjectRestrictNode)) {
            return updateNode;
        }

        ResultSetNode pr2 = ((ProjectRestrictNode) pr1).getChildResult();
        if (!(pr2 instanceof ProjectRestrictNode)) {
            return updateNode;
        }

        ResultSetNode fromBaseTable = ((ProjectRestrictNode) pr2).getChildResult();
        if (!(fromBaseTable instanceof FromBaseTable)) {
            return updateNode;
        }

        insertBatchOnceNode(updateNode, prUnderUpdate, subqueryNode, correlatedSubqueryColRefList);
        return updateNode;
    }

    /**
     * From: Update -> ProjectRestrict -> Source
     *                        \
     *                         Subquery
     *
     * To  : Update -> BatchOnce -> Source
     *                     \
     *                     Subquery
     */
    private void insertBatchOnceNode(UpdateNode updateNode,
                                     ProjectRestrictNode prUnderUpdate,
                                     SubqueryNode subqueryNode,
                                     List<Pair<ColumnReference, ColumnReference>> correlatedSubqueryColRefList) throws StandardException {

        /* Remove predicates from subquery table */
        ProjectRestrictNode subPrNode = (ProjectRestrictNode) subqueryNode.getResultSet();
        ProjectRestrictNode prLevel2 = (ProjectRestrictNode) subPrNode.getChildResult();
        FromBaseTable subFromBaseTable = (FromBaseTable) prLevel2.getChildResult();
        subFromBaseTable.clearAllPredicates();

        /* Batch once is over existing project restrict */
        ResultSetNode newSourceNode = prUnderUpdate.getChildResult();
        int[] sourceCorrelatedColumnPositions = new int[correlatedSubqueryColRefList.size()];
        int[] subqueryCorrelatedColumnPositions = new int[correlatedSubqueryColRefList.size()];
        for (int i = 0; i < correlatedSubqueryColRefList.size(); ++i) {
            Pair<ColumnReference, ColumnReference> correlatedSubqueryColRef = correlatedSubqueryColRefList.get(i);
            int sourceCorrelatedColumnPosition = findColRefPosition(correlatedSubqueryColRef.getFirst(), newSourceNode);
            int subqueryCorrelatedColumnPosition = findColRefPosition(correlatedSubqueryColRef.getSecond(), subFromBaseTable);
            sourceCorrelatedColumnPositions[i] = sourceCorrelatedColumnPosition;
            subqueryCorrelatedColumnPositions[i] = subqueryCorrelatedColumnPosition;
        }
        BatchOnceNode batchOnceNode = (BatchOnceNode) updateNode.getNodeFactory().getNode(
                C_NodeTypes.BATCH_ONCE_NODE,
                updateNode.getContextManager());

        batchOnceNode.init(newSourceNode,
                subqueryNode,
                sourceCorrelatedColumnPositions,
                subqueryCorrelatedColumnPositions);

        /* Update node is over batch once */
        updateNode.init(batchOnceNode);

    }

    /**
     * For now we do BatchOnce only if the subquery looks like this:
     *
     * <pre>
     *       SubqueryNode
     *       |
     *       PRN
     *       |
     *       PRN
     *       |
     *       FromBaseTable
     *       |
     *       And
     *       |
     *       BRON
     *      / \
     *     CR  CR
     * </pre>
     *
     * Where the BRON is equality and one of the CR is correlated and the other is not.  This method returns
     * NULL if the tree does not look like this.  If it does in returns a pair of column references where
     * the first element in the pair is the correlated column reference.
     */
    private List<Pair<ColumnReference,ColumnReference>> isApplicableSubqueryNode(SubqueryNode subqueryNode) throws StandardException {

        /*
         * Must be expression subquery
         */
        if (subqueryNode.getSubqueryType() != SubqueryNode.EXPRESSION_SUBQUERY) {
            return null;
        }

        /*
         * Two PRN for some reason.
         */
        if (!(subqueryNode.getResultSet() instanceof ProjectRestrictNode)) {
            return null;
        }
        ProjectRestrictNode pr1 = (ProjectRestrictNode) subqueryNode.getResultSet();
        if (!(pr1.getChildResult() instanceof ProjectRestrictNode)) {
            return null;
        }
        ProjectRestrictNode pr2 = (ProjectRestrictNode) pr1.getChildResult();
        if (!(pr2.getChildResult() instanceof FromBaseTable)) {
            return null;
        }


        FromBaseTable fromBaseTable = (FromBaseTable) pr2.getChildResult();
        PredicateList predicateList = RSUtils.getPreds(fromBaseTable);
        List<Pair<ColumnReference,ColumnReference>> result = Lists.newArrayList();
        for (int i = 0; i < predicateList.size(); ++i) {
            /*
             * Must be equality BRON
             */
            Predicate predicate = predicateList.elementAt(i);
            AndNode and = predicate.getAndNode();
            if (!(and.getLeftOperand() instanceof BinaryRelationalOperatorNode)) {
                return null;
            }
            BinaryRelationalOperatorNode bron = (BinaryRelationalOperatorNode) and.getLeftOperand();
            if (bron.getOperator() != RelationalOperator.EQUALS_RELOP) {
                return null;
            }

            /**
             * With one correlated CR
             */
            ValueNode leftOperand = bron.getLeftOperand();
            ValueNode rightOperand = bron.getRightOperand();
            if (!(leftOperand instanceof ColumnReference && rightOperand instanceof ColumnReference)) {
                return null;
            }

            ColumnReference leftCr = (ColumnReference) leftOperand;
            ColumnReference rightCr = (ColumnReference) rightOperand;

            if (leftCr.getCorrelated()) {
                result.add(Pair.newPair(leftCr, rightCr));
            } else if (rightCr.getCorrelated()) {
                result.add(Pair.newPair(rightCr, leftCr));
            }
        }
        return result;
    }

    /**
     * Returns the 1-based index of the specified column reference with the given results set's result columns.
     */
    private static int findColRefPosition(ColumnReference columnReference, ResultSetNode resultSetNode) {
        ResultColumnList sourceResultColumns = resultSetNode.getResultColumns();
        int position = 1;
        for (ResultColumn rc : sourceResultColumns) {
            if (rc.getVirtualColumnId() == columnReference.getSource().getVirtualColumnId()) {
                break;
            }
            position++;
        }
        return position;
    }

    /**
     * Returns the 1-based index of the RowLocation column within the specified result set's result columns. Returns
     * -1 if there is no CurrentRowLocation column in the specified result set.
     */
    private static int findRowLocationPosition(ResultSetNode resultSetNode) {
        ResultColumnList resultColumns = resultSetNode.getResultColumns();
        int position = 1;
        for (ResultColumn rc : resultColumns) {
            ValueNode expression = rc.getExpression();
            if (expression instanceof VirtualColumnNode) {
                VirtualColumnNode expression1 = (VirtualColumnNode) expression;
                if (expression1.getSourceColumn().getExpression() instanceof CurrentRowLocationNode) {
                    return position;
                }
            }
            position++;
        }
        return -1;
    }

}
